/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.nfa;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import static org.apache.flink.cep.nfa.MigrationUtils.deserializeComputationStates;

/**
 * 非确定有限状态机实现
 * Non-deterministic finite automaton implementation.
 *
 *
 * keyed 模式的输入流，cep operator为每一个key保留一个NFA,非keyed模式，使用一个全局的NFA。处理事件时，它会更新NFA的内部状态机。
 * 属于部分匹配序列的事件保存在内部缓冲区SharedBuffer中，这是一种内存优化的数据结构，正是为了实现这一目的。
 * 当包含缓冲区中事件的所有匹配序列满足以下条件时，缓冲区中的事件将被删除：
 *
 *
 * <p>The {@link org.apache.flink.cep.operator.CepOperator CEP operator} keeps one NFA per key, for
 * keyed input streams, and a single global NFA for non-keyed ones. When an event gets processed, it
 * updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal {@link
 * SharedBuffer buffer}, which is a memory-optimized data-structure exactly for this purpose. Events
 * in the buffer are removed when all the matched sequences that contain them are:
 *
 * <ol>
 *   <li>emitted (success) 发送到下一个状态
 *   <li>discarded (patterns containing NOT) 丢弃
 *   <li>timed-out (windowed patterns) 超时
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event
 * Streams".
 *
 * @param <T> Type of the processed events
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 *     https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA<T> {

    /**
     * NFA编译器返回的所有有效NFA状态的集合。这些直接来自用户指定的模式。
     * A set of all the valid NFA states, as returned by the {@link NFACompiler NFACompiler}. These
     * are directly derived from the user-specified pattern.
     */
    private final Map<String, State<T>> states;

    /**
     * pattern的窗口的长度，由Pattern#within(Time)指定
     * The length of a windowed pattern, as specified using the {@link
     * org.apache.flink.cep.pattern.Pattern#within(Time)} Pattern.within(Time)} method.
     */
    private final long windowTime;

    /**
     * A flag indicating if we want timed-out patterns (in case of windowed patterns) to be emitted
     * ({@code true}), or silently discarded ({@code false}).
     */
    private final boolean handleTimeout;

    public NFA(
            final Collection<State<T>> validStates,
            final long windowTime,
            final boolean handleTimeout) {
        this.windowTime = windowTime;
        this.handleTimeout = handleTimeout;
        //加载所有的状态
        this.states = loadStates(validStates);
    }

    private Map<String, State<T>> loadStates(final Collection<State<T>> validStates) {
        //<状态名称，状态>
        Map<String, State<T>> tmp = new HashMap<>(4);
        for (State<T> state : validStates) {
            tmp.put(state.getName(), state);
        }
        return Collections.unmodifiableMap(tmp);
    }

    @VisibleForTesting
    public Collection<State<T>> getStates() {
        return states.values();
    }

    public NFAState createInitialNFAState() {
        //表示的是一系列当前匹配到的计算状态，每一个状态在拿到下一个元素的时候都会根据condition判断自己是能够继续往下匹配生成下一个computation state还是匹配失败。
        Queue<ComputationState> startingStates = new LinkedList<>();
        for (State<T> state : states.values()) {
            if (state.isStart()) {
                //找出所有的初始状态
                startingStates.add(ComputationState.createStartState(state.getName()));
            }
        }
        //初始状态放入半匹配优先级队列中
        return new NFAState(startingStates);
    }

    /**
     * 从状态机的状态中根据状态名称获取状态
     * @param state
     * @return
     */
    private State<T> getState(ComputationState state) {
        return states.get(state.getCurrentStateName());
    }

    private boolean isStartState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }

        return stateObject.isStart();
    }

    private boolean isStopState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }

        return stateObject.isStop();
    }

    private boolean isFinalState(ComputationState state) {
        State<T> stateObject = getState(state);
        if (stateObject == null) {
            throw new FlinkRuntimeException(
                    "State "
                            + state.getCurrentStateName()
                            + " does not exist in the NFA. NFA has states "
                            + states.values());
        }

        return stateObject.isFinal();
    }

    /**
     * NFA的初始化方法。在传递任何元素之前调用它，因此适合一次性设置工作。
     *
     * Initialization method for the NFA. It is called before any element is passed and thus
     * suitable for one time setup work.
     *
     * @param cepRuntimeContext runtime context of the enclosing operator
     * @param conf The configuration containing the parameters attached to the contract.
     */
    public void open(RuntimeContext cepRuntimeContext, Configuration conf) throws Exception {
        //遍历状态机中所有的状态 （顶点）
        for (State<T> state : getStates()) {
            //遍历每个状态的转换条件
            for (StateTransition<T> transition : state.getStateTransitions()) {
                //转换条件
                IterativeCondition condition = transition.getCondition();
                //给每个条件设置运行时上下文
                FunctionUtils.setFunctionRuntimeContext(condition, cepRuntimeContext);
                //运行条件
                FunctionUtils.openFunction(condition, conf);
            }
        }
    }

    /** Tear-down method for the NFA. */
    public void close() throws Exception {
        for (State<T> state : getStates()) {
            for (StateTransition<T> transition : state.getStateTransitions()) {
                IterativeCondition condition = transition.getCondition();
                FunctionUtils.closeFunction(condition);
            }
        }
    }

    /**
     * 处理下一个输入事件。如果部分计算达到最终状态，则返回生成的事件序列。如果计算超时和超时处理被激活，则会返回超时事件模式。
     * 如果计算达到停止状态，则丢弃前进路径，并返回当前构造的路径以及导致停止状态的元素。
     *
     * Processes the next input event. If some of the computations reach a final state then the
     * resulting event sequences are returned. If computations time out and timeout handling is
     * activated, then the timed out event patterns are returned.
     *
     * <p>If computations reach a stop state, the path forward is discarded and currently
     * constructed path is returned with the element that resulted in the stop state.
     *
     * @param sharedBufferAccessor the accessor to SharedBuffer object that we need to work upon
     *     while processing
     * @param nfaState The NFAState object that we need to affect while processing
     * @param event The current event to be processed or null if only pruning shall be done
     * @param timestamp The timestamp of the current event
     * @param afterMatchSkipStrategy The skip strategy to use after per match
     * @param timerService gives access to processing time and time characteristic, needed for
     *     condition evaluation
     * @return Tuple of the collection of matched patterns (e.g. the result of computations which
     *     have reached a final state) and the collection of timed out patterns (if timeout handling
     *     is activated)
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Collection<Map<String, List<T>>> process(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final T event,
            final long timestamp,
            final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final TimerService timerService)
            throws Exception {
        try (EventWrapper eventWrapper = new EventWrapper(event, timestamp, sharedBufferAccessor)) {
            return doProcess(
                    sharedBufferAccessor,
                    nfaState,
                    eventWrapper,
                    afterMatchSkipStrategy,
                    timerService);
        }
    }

    /**
     * Prunes states assuming there will be no events with timestamp <b>lower</b> than the given
     * one. It clears the sharedBuffer and also emits all timed out partial matches.
     *
     * @param sharedBufferAccessor the accessor to SharedBuffer object that we need to work upon
     *     while processing
     * @param nfaState The NFAState object that we need to affect while processing
     * @param timestamp timestamp that indicates that there will be no more events with lower
     *     timestamp
     * @return all timed outed partial matches
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Collection<Tuple2<Map<String, List<T>>, Long>> advanceTime(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final long timestamp)
            throws Exception {

        final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();
        final PriorityQueue<ComputationState> newPartialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

        for (ComputationState computationState : nfaState.getPartialMatches()) {
            if (isStateTimedOut(computationState, timestamp)) {

                if (handleTimeout) {
                    // extract the timed out event pattern
                    Map<String, List<T>> timedOutPattern =
                            sharedBufferAccessor.materializeMatch(
                                    extractCurrentMatches(sharedBufferAccessor, computationState));
                    timeoutResult.add(
                            Tuple2.of(
                                    timedOutPattern,
                                    computationState.getStartTimestamp() + windowTime));
                }

                sharedBufferAccessor.releaseNode(
                        computationState.getPreviousBufferEntry(), computationState.getVersion());

                nfaState.setStateChanged();
            } else {
                newPartialMatches.add(computationState);
            }
        }

        nfaState.setNewPartialMatches(newPartialMatches);

        sharedBufferAccessor.advanceTime(timestamp);

        return timeoutResult;
    }

    private boolean isStateTimedOut(final ComputationState state, final long timestamp) {
        return !isStartState(state)
                && windowTime > 0L
                && timestamp - state.getStartTimestamp() >= windowTime;
    }

    /**
     * nfa处理逻辑
     * @param sharedBufferAccessor
     * @param nfaState
     * @param event
     * @param afterMatchSkipStrategy
     * @param timerService
     * @return
     * @throws Exception
     */
    private Collection<Map<String, List<T>>> doProcess(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final NFAState nfaState,
            final EventWrapper event,
            final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final TimerService timerService)
            throws Exception {

        //局部匹配队列 （已经匹配了的state队列）需要匹配的状态队列？
        final PriorityQueue<ComputationState> newPartialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
        //潜在匹配队列  匹配完成的状态队列？
        final PriorityQueue<ComputationState> potentialMatches =
                new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

        // 遍历状态机中当前所有的状态集
        // iterate over all current computations
        for (ComputationState computationState : nfaState.getPartialMatches()) {
            //根据给定的计算状态、当前事件、其时间戳和内部状态机计算下一个计算状态，并存储匹配事件到缓存中
            //TODO 这里是核心处理逻辑
            final Collection<ComputationState> newComputationStates =
                    computeNextStates(sharedBufferAccessor, computationState, event, timerService);

            if (newComputationStates.size() != 1) {
                nfaState.setStateChanged();
            } else if (!newComputationStates.iterator().next().equals(computationState)) {
                nfaState.setStateChanged();
            }

            //延迟添加新的计算状态，以防达到停止状态并放弃这条路径。
            // delay adding new computation states in case a stop state is reached and we discard
            // the path.
            final Collection<ComputationState> statesToRetain = new ArrayList<>();
            //是否在此路径中达到停止状态
            // if stop state reached in this path
            boolean shouldDiscardPath = false;
            for (final ComputationState newComputationState : newComputationStates) {

                if (isFinalState(newComputationState)) {
                    //如果是final state，添加到potentialMatches
                    potentialMatches.add(newComputationState);
                } else if (isStopState(newComputationState)) {
                    //已达到停止状态。释放共享缓存sharedBuffer中 停止状态的条目
                    // reached stop state. release entry for the stop state
                    shouldDiscardPath = true;
                    sharedBufferAccessor.releaseNode(
                            newComputationState.getPreviousBufferEntry(),
                            newComputationState.getVersion());
                } else {
                    //添加新的计算状态；它将在下一个事件到达后进行处理
                    // add new computation state; it will be processed once the next event arrives
                    statesToRetain.add(newComputationState);
                }
            }

            if (shouldDiscardPath) {
                //此分支中已达到停止状态。释放导致从缓冲区中删除上一个事件的分支
                // a stop state was reached in this branch. release branch which results in removing
                // previous event from
                // the buffer
                for (final ComputationState state : statesToRetain) {
                    sharedBufferAccessor.releaseNode(
                            state.getPreviousBufferEntry(), state.getVersion());
                }
            } else {
                newPartialMatches.addAll(statesToRetain);
            }
        }

        if (!potentialMatches.isEmpty()) {
            nfaState.setStateChanged();
        }

        List<Map<String, List<T>>> result = new ArrayList<>();
        if (afterMatchSkipStrategy.isSkipStrategy()) {
            processMatchesAccordingToSkipStrategy(
                    sharedBufferAccessor,
                    nfaState,
                    afterMatchSkipStrategy,
                    potentialMatches,
                    newPartialMatches,
                    result);
        } else {
            //根据potentialMatches中到达finish状态的ComputationState来回溯匹配完成的事件序列
            for (ComputationState match : potentialMatches) {
                //TODO 根据匹配完成的状态列表PartialMatches 回溯出事件序列
                Map<String, List<T>> materializedMatch =
                        sharedBufferAccessor.materializeMatch(
                                sharedBufferAccessor
                                        .extractPatterns(
                                                match.getPreviousBufferEntry(), match.getVersion())
                                        .get(0));

                result.add(materializedMatch);
                sharedBufferAccessor.releaseNode(
                        match.getPreviousBufferEntry(), match.getVersion());
            }
        }

        nfaState.setNewPartialMatches(newPartialMatches);

        return result;
    }

    private void processMatchesAccordingToSkipStrategy(
            SharedBufferAccessor<T> sharedBufferAccessor,
            NFAState nfaState,
            AfterMatchSkipStrategy afterMatchSkipStrategy,
            PriorityQueue<ComputationState> potentialMatches,
            PriorityQueue<ComputationState> partialMatches,
            List<Map<String, List<T>>> result)
            throws Exception {

        nfaState.getCompletedMatches().addAll(potentialMatches);

        ComputationState earliestMatch = nfaState.getCompletedMatches().peek();

        if (earliestMatch != null) {

            ComputationState earliestPartialMatch;
            while (earliestMatch != null
                    && ((earliestPartialMatch = partialMatches.peek()) == null
                            || isEarlier(earliestMatch, earliestPartialMatch))) {

                nfaState.setStateChanged();
                nfaState.getCompletedMatches().poll();
                List<Map<String, List<EventId>>> matchedResult =
                        sharedBufferAccessor.extractPatterns(
                                earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());

                afterMatchSkipStrategy.prune(partialMatches, matchedResult, sharedBufferAccessor);

                afterMatchSkipStrategy.prune(
                        nfaState.getCompletedMatches(), matchedResult, sharedBufferAccessor);

                result.add(sharedBufferAccessor.materializeMatch(matchedResult.get(0)));
                sharedBufferAccessor.releaseNode(
                        earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());
                earliestMatch = nfaState.getCompletedMatches().peek();
            }

            nfaState.getPartialMatches()
                    .removeIf(pm -> pm.getStartEventID() != null && !partialMatches.contains(pm));
        }
    }

    private boolean isEarlier(
            ComputationState earliestMatch, ComputationState earliestPartialMatch) {
        return NFAState.COMPUTATION_STATE_COMPARATOR.compare(earliestMatch, earliestPartialMatch)
                <= 0;
    }

    /**
     * 是否是相等的状态
     * @param s1
     * @param s2
     * @param <T>
     * @return
     */
    private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
        return s1.getName().equals(s2.getName());
    }

    /**
     * 用于存储已解决的转换的类。它在插入时统计忽略和采取操作的分支转换数。
     * Class for storing resolved transitions. It counts at insert time the number of branching
     * transitions both for IGNORE and TAKE actions.
     */
    private static class OutgoingEdges<T> {
        private List<StateTransition<T>> edges = new ArrayList<>();

        private final State<T> currentState;

        private int totalTakeBranches = 0;
        private int totalIgnoreBranches = 0;

        OutgoingEdges(final State<T> currentState) {
            this.currentState = currentState;
        }

        /**
         * 统计ignor和take
         * @param edge
         */
        void add(StateTransition<T> edge) {

            if (!isSelfIgnore(edge)) {
                if (edge.getAction() == StateTransitionAction.IGNORE) {
                    totalIgnoreBranches++;
                } else if (edge.getAction() == StateTransitionAction.TAKE) {
                    totalTakeBranches++;
                }
            }

            edges.add(edge);
        }

        int getTotalIgnoreBranches() {
            return totalIgnoreBranches;
        }

        int getTotalTakeBranches() {
            return totalTakeBranches;
        }

        List<StateTransition<T>> getEdges() {
            return edges;
        }

        private boolean isSelfIgnore(final StateTransition<T> edge) {
            return isEquivalentState(edge.getTargetState(), currentState)
                    && edge.getAction() == StateTransitionAction.IGNORE;
        }
    }

    /**
     * Helper class that ensures event is registered only once throughout the life of this object
     * and released on close of this object. This allows to wrap whole processing of the event with
     * try-with-resources block.
     */
    private class EventWrapper implements AutoCloseable {

        private final T event;

        private long timestamp;

        private final SharedBufferAccessor<T> sharedBufferAccessor;

        private EventId eventId;

        EventWrapper(T event, long timestamp, SharedBufferAccessor<T> sharedBufferAccessor) {
            this.event = event;
            this.timestamp = timestamp;
            this.sharedBufferAccessor = sharedBufferAccessor;
        }

        EventId getEventId() throws Exception {
            if (eventId == null) {
                this.eventId = sharedBufferAccessor.registerEvent(event, timestamp);
            }

            return eventId;
        }

        T getEvent() {
            return event;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public void close() throws Exception {
            if (eventId != null) {
                sharedBufferAccessor.releaseEvent(eventId);
            }
        }
    }

    /**
     * 根据给定的计算状态、当前事件、其时间戳和内部状态机计算下一个计算状态。算法是：
     * Computes the next computation states based on the given computation state, the current event,
     * its timestamp and the internal state machine. The algorithm is:
     *
     * <ol>
     *   <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}
     *   <li>Perform transitions:
     *       <ol>
     *         <li>IGNORE (links in {@link SharedBuffer} will still point to the previous event)
     *             <ul>
     *               <li>do not perform for Start State - special case
     *               <li>if stays in the same state increase the current stage for future use with
     *                   number of outgoing edges
     *               <li>if after PROCEED increase current stage and add new stage (as we change the
     *                   state)
     *               <li>lock the entry in {@link SharedBuffer} as it is needed in the created
     *                   branch
     *             </ul>
     *         <li>TAKE (links in {@link SharedBuffer} will point to the current event)
     *             <ul>
     *               <li>add entry to the shared buffer with version of the current computation
     *                   state
     *               <li>add stage and then increase with number of takes for the future computation
     *                   states
     *               <li>peek to the next state if it has PROCEED path to a Final State, if true
     *                   create Final ComputationState to emit results
     *             </ul>
     *       </ol>
     *   <li>Handle the Start State, as it always have to remain
     *   <li>Release the corresponding entries in {@link SharedBuffer}.
     * </ol>
     *
     * @param sharedBufferAccessor The accessor to shared buffer that we need to change
     * @param computationState Current computation state
     * @param event Current event which is processed
     * @param timerService timer service which provides access to time related features
     * @return Collection of computation states which result from the current one
     * @throws Exception Thrown if the system cannot access the state.
     */
    private Collection<ComputationState> computeNextStates(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final ComputationState computationState,
            final EventWrapper event,
            final TimerService timerService)
            throws Exception {

        final ConditionContext context =
                new ConditionContext(
                        sharedBufferAccessor, computationState, timerService, event.getTimestamp());

        //创建决策图 当前状态计算结果，或者继续往下一个节点计算状态
        //根据当前的computationState和事件计算出事件的所有迁移边StateTransition
        final OutgoingEdges<T> outgoingEdges =
                createDecisionGraph(context, computationState, event.getEvent());

        //基于之前计算的边创建计算版本
        //我们需要推迟计算状态的创建，直到我们知道有多少条边开始
        //在这种计算状态下，我们可以分配适当的版本
        // Create the computing version based on the previously computed edges
        // We need to defer the creation of computation states until we know how many edges start
        // at this computation state so that we can assign proper version
        final List<StateTransition<T>> edges = outgoingEdges.getEdges();
        int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
        int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
        int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

        //TODO 这里应该是要分裂？
        final List<ComputationState> resultingComputationStates = new ArrayList<>();
        //遍历所有的迁移边
        for (StateTransition<T> edge : edges) {
            switch (edge.getAction()) {
                case IGNORE:
                    {
                        if (!isStartState(computationState)) {
                            final DeweyNumber version;
                            if (isEquivalentState(
                                    edge.getTargetState(), getState(computationState))) {
                                // Stay in the same state (it can be either looping one or
                                // singleton)
                                final int toIncrease =
                                        calculateIncreasingSelfState(
                                                outgoingEdges.getTotalIgnoreBranches(),
                                                outgoingEdges.getTotalTakeBranches());
                                version = computationState.getVersion().increase(toIncrease);
                            } else {
                                // IGNORE after PROCEED
                                version =
                                        computationState
                                                .getVersion()
                                                .increase(totalTakeToSkip + ignoreBranchesToVisit)
                                                .addStage();
                                ignoreBranchesToVisit--;
                            }

                            addComputationState(
                                    sharedBufferAccessor,
                                    resultingComputationStates,
                                    edge.getTargetState(),
                                    computationState.getPreviousBufferEntry(),
                                    version,
                                    computationState.getStartTimestamp(),
                                    computationState.getStartEventID());
                        }
                    }
                    break;
                case TAKE:
                    //下一个state
                    final State<T> nextState = edge.getTargetState();
                    //当前state
                    final State<T> currentState = edge.getSourceState();

                    //TODO
                    final NodeId previousEntry = computationState.getPreviousBufferEntry();

                    //根据take的数量增加版本号
                    final DeweyNumber currentVersion =
                            computationState.getVersion().increase(takeBranchesToVisit);
                    //take事件之后增加版本的stage，即增加版本号长度，之前版本为该版本的前缀用于回溯时判断是否是在同一个run中。后面会举例说明。
                    final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
                    takeBranchesToVisit--;

                    //添加到sharedBuffer中，返回此事件在缓存中的id
                    //把当前的事件存入sharedBuffer 并且指向previousEntry 版本号为currentVersion 用于回溯事件序列
                    final NodeId newEntry =
                            sharedBufferAccessor.put(
                                    currentState.getName(),
                                    event.getEventId(),
                                    previousEntry,
                                    currentVersion);

                    final long startTimestamp;
                    final EventId startEventId;
                    if (isStartState(computationState)) {
                        startTimestamp = event.getTimestamp();
                        startEventId = event.getEventId();
                    } else {
                        startTimestamp = computationState.getStartTimestamp();
                        startEventId = computationState.getStartEventID();
                    }

                    //添加计算状态到 resultingComputationStates，更新缓存中事件的引用计数
                    //更新当前的ComputationState previousEntry更新为newEntry(当前到达的)，版本为增加stage的版本nextVersion，state为take后的nextState
                    addComputationState(
                            sharedBufferAccessor,
                            resultingComputationStates,
                            nextState,
                            newEntry,
                            nextVersion,
                            startTimestamp,
                            startEventId);

                    //检查新创建的状态是否可选（有一个到最终状态的PROCEED路径）
                    // check if newly created state is optional (have a PROCEED path to Final state)
                    //遍历nextState状态的转换，获取可达的final状态
                    final State<T> finalState =
                            findFinalStateAfterProceed(context, nextState, event.getEvent());
                    if (finalState != null) {
                        addComputationState(
                                sharedBufferAccessor,
                                resultingComputationStates,
                                finalState,
                                newEntry,
                                nextVersion,
                                startTimestamp,
                                startEventId);
                    }
                    break;
            }
        }

        if (isStartState(computationState)) {
            int totalBranches =
                    calculateIncreasingSelfState(
                            outgoingEdges.getTotalIgnoreBranches(),
                            outgoingEdges.getTotalTakeBranches());

            DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
            ComputationState startState =
                    ComputationState.createStartState(
                            computationState.getCurrentStateName(), startVersion);
            resultingComputationStates.add(startState);
        }

        if (computationState.getPreviousBufferEntry() != null) {
            // release the shared entry referenced by the current computation state.
            sharedBufferAccessor.releaseNode(
                    computationState.getPreviousBufferEntry(), computationState.getVersion());
        }

        //返回更新后的ComputationState
        return resultingComputationStates;
    }

    private void addComputationState(
            SharedBufferAccessor<T> sharedBufferAccessor,
            List<ComputationState> computationStates,
            State<T> currentState,
            NodeId previousEntry,
            DeweyNumber version,
            long startTimestamp,
            EventId startEventId)
            throws Exception {
        ComputationState computationState =
                ComputationState.createState(
                        currentState.getName(),
                        previousEntry,
                        version,
                        startTimestamp,
                        startEventId);
        computationStates.add(computationState);

        sharedBufferAccessor.lockNode(previousEntry, computationState.getVersion());
    }

    /**
     * 获取状态Proceed之后的final状态
     */
    private State<T> findFinalStateAfterProceed(ConditionContext context, State<T> state, T event) {
        //等待检查的状态 栈
        final Stack<State<T>> statesToCheck = new Stack<>();
        statesToCheck.push(state);
        try {
            while (!statesToCheck.isEmpty()) {
                final State<T> currentState = statesToCheck.pop();
                //遍历状态的状态转换  顶点的边
                for (StateTransition<T> transition : currentState.getStateTransitions()) {
                    //如果状态转换动作为PROCEED && 边的条件满足
                    if (transition.getAction() == StateTransitionAction.PROCEED
                            && checkFilterCondition(context, transition.getCondition(), event)) {
                        if (transition.getTargetState().isFinal()) {
                            //如果状态的下一个状态为final状态，则返回此final状态
                            return transition.getTargetState();
                        } else {
                            //否则把下一个状态压栈，继续循环找下一个
                            statesToCheck.push(transition.getTargetState());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failure happened in filter function.", e);
        }

        return null;
    }

    private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
        return takeBranches == 0 && ignoreBranches == 0
                ? 0
                : ignoreBranches + Math.max(1, takeBranches);
    }

    /**
     *
     * @param context
     * @param computationState
     * @param event
     * @return 当前状态上，所有的状态转换中，满足condition条件，且action为take的状态转换 StateTransition
     */
    private OutgoingEdges<T> createDecisionGraph(
            ConditionContext context, ComputationState computationState, T event) {
        //获取当前状态
        State<T> state = getState(computationState);
        final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);

        final Stack<State<T>> states = new Stack<>();
        //入栈
        states.push(state);

        //首先创建所有出边，以便能够对杜威版本进行推理
        // First create all outgoing edges, so to be able to reason about the Dewey version
        //只要状态栈不为空，就一直向后处理
        while (!states.isEmpty()) {
            State<T> currentState = states.pop();
            //获取当前状态的转换集合 （顶点的多条边）
            Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

            //遍历当前状态的转换，检查每个状态的所有状态转换
            // check all state transitions for each state
            for (StateTransition<T> stateTransition : stateTransitions) {
                try {
                    //如果filter条件计算为true或者条件为空，根据状态转换的动作进行对应的处理 （用户的condition条件为true）
                    if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
                        // filter condition is true
                        switch (stateTransition.getAction()) {
                            case PROCEED:
                                // simply advance the computation state, but apply the current event
                                // to it
                                // PROCEED is equivalent to an epsilon transition
                                //获取下一个状态 入栈，然后继续处理（外层是个while true的，只要栈不为空，会一直向下一个状态走）
                                states.push(stateTransition.getTargetState());
                                break;
                            case IGNORE:
                            case TAKE:
                                //状态转换添加到 outgoingEdges
                                outgoingEdges.add(stateTransition);
                                break;
                        }
                    }
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Failure happened in filter function.", e);
                }
            }
        }
        return outgoingEdges;
    }

    /**
     * 计算用户的condition是否满足
     * @param context
     * @param condition
     * @param event
     * @return
     * @throws Exception
     */
    private boolean checkFilterCondition(
            ConditionContext context, IterativeCondition<T> condition, T event) throws Exception {
        return condition == null || condition.filter(event, context);
    }

    /**
     * 提取从开始到给定计算状态的所有事件序列。事件序列作为映射返回，其中包含事件和事件映射到的状态的名称。
     * Extracts all the sequences of events from the start to the given computation state. An event
     * sequence is returned as a map which contains the events and the names of the states to which
     * the events were mapped.
     *
     * @param sharedBufferAccessor The accessor to {@link SharedBuffer} from which to extract the
     *     matches
     * @param computationState The end computation state of the extracted event sequences
     * @return Collection of event sequences which end in the given computation state
     * @throws Exception Thrown if the system cannot access the state.
     */
    private Map<String, List<EventId>> extractCurrentMatches(
            final SharedBufferAccessor<T> sharedBufferAccessor,
            final ComputationState computationState)
            throws Exception {
        if (computationState.getPreviousBufferEntry() == null) {
            return new HashMap<>();
        }

        List<Map<String, List<EventId>>> paths =
                sharedBufferAccessor.extractPatterns(
                        computationState.getPreviousBufferEntry(), computationState.getVersion());

        if (paths.isEmpty()) {
            return new HashMap<>();
        }
        // for a given computation state, we cannot have more than one matching patterns.
        Preconditions.checkState(paths.size() == 1);

        return paths.get(0);
    }

    /** The context used when evaluating this computation state. */
    private class ConditionContext implements IterativeCondition.Context<T> {

        private final TimerService timerService;

        private final long eventTimestamp;

        /** The current computation state. */
        private ComputationState computationState;

        /**
         * The matched pattern so far. A condition will be evaluated over this pattern. This is
         * evaluated <b>only once</b>, as this is an expensive operation that traverses a path in
         * the {@link SharedBuffer}.
         */
        private Map<String, List<T>> matchedEvents;

        private SharedBufferAccessor<T> sharedBufferAccessor;

        ConditionContext(
                final SharedBufferAccessor<T> sharedBufferAccessor,
                final ComputationState computationState,
                final TimerService timerService,
                final long eventTimestamp) {
            this.computationState = computationState;
            this.sharedBufferAccessor = sharedBufferAccessor;
            this.timerService = timerService;
            this.eventTimestamp = eventTimestamp;
        }

        @Override
        public Iterable<T> getEventsForPattern(final String key) throws Exception {
            Preconditions.checkNotNull(key);

            // the (partially) matched pattern is computed lazily when this method is called.
            // this is to avoid any overheads when using a simple, non-iterative condition.

            if (matchedEvents == null) {
                this.matchedEvents =
                        sharedBufferAccessor.materializeMatch(
                                extractCurrentMatches(sharedBufferAccessor, computationState));
            }

            return new Iterable<T>() {
                @Override
                public Iterator<T> iterator() {
                    List<T> elements = matchedEvents.get(key);
                    return elements == null
                            ? Collections.EMPTY_LIST.<T>iterator()
                            : elements.iterator();
                }
            };
        }

        @Override
        public long timestamp() {
            return eventTimestamp;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    ////////////////////				DEPRECATED/MIGRATION UTILS

    /** Wrapper for migrated state. */
    public static class MigratedNFA<T> {

        private final Queue<ComputationState> computationStates;
        private final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer;

        public org.apache.flink.cep.nfa.SharedBuffer<T> getSharedBuffer() {
            return sharedBuffer;
        }

        public Queue<ComputationState> getComputationStates() {
            return computationStates;
        }

        MigratedNFA(
                final Queue<ComputationState> computationStates,
                final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer) {
            this.sharedBuffer = sharedBuffer;
            this.computationStates = computationStates;
        }
    }

    /**
     * @deprecated This snapshot class is no longer in use, and only maintained for backwards
     *     compatibility purposes. It is fully replaced by {@link MigratedNFASerializerSnapshot}.
     */
    @Deprecated
    public static final class NFASerializerConfigSnapshot<T>
            extends CompositeTypeSerializerConfigSnapshot<MigratedNFA<T>> {

        private static final int VERSION = 1;

        /** This empty constructor is required for deserializing the configuration. */
        public NFASerializerConfigSnapshot() {}

        public NFASerializerConfigSnapshot(
                TypeSerializer<T> eventSerializer,
                TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {

            super(eventSerializer, sharedBufferSerializer);
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public TypeSerializerSchemaCompatibility<MigratedNFA<T>> resolveSchemaCompatibility(
                TypeSerializer<MigratedNFA<T>> newSerializer) {
            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer,
                    new MigratedNFASerializerSnapshot<>(),
                    getNestedSerializerSnapshots());
        }
    }

    /** A {@link TypeSerializerSnapshot} for the legacy {@link NFASerializer}. */
    @SuppressWarnings("deprecation")
    public static final class MigratedNFASerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<MigratedNFA<T>, NFASerializer<T>> {

        private static final int VERSION = 2;

        public MigratedNFASerializerSnapshot() {
            super(NFASerializer.class);
        }

        MigratedNFASerializerSnapshot(NFASerializer<T> legacyNfaSerializer) {
            super(legacyNfaSerializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(NFASerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.eventSerializer, outerSerializer.sharedBufferSerializer
            };
        }

        @Override
        protected NFASerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> eventSerializer = (TypeSerializer<T>) nestedSerializers[0];

            @SuppressWarnings("unchecked")
            TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer =
                    (TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>>) nestedSerializers[1];

            return new NFASerializer<>(eventSerializer, sharedBufferSerializer);
        }
    }

    /** Only for backward compatibility with <=1.5. */
    @Deprecated
    public static class NFASerializer<T> extends TypeSerializer<MigratedNFA<T>> {

        private static final long serialVersionUID = 2098282423980597010L;

        private final TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>>
                sharedBufferSerializer;

        private final TypeSerializer<T> eventSerializer;

        public NFASerializer(TypeSerializer<T> typeSerializer) {
            this(
                    typeSerializer,
                    new org.apache.flink.cep.nfa.SharedBuffer.SharedBufferSerializer<>(
                            StringSerializer.INSTANCE, typeSerializer));
        }

        NFASerializer(
                TypeSerializer<T> typeSerializer,
                TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {
            this.eventSerializer = typeSerializer;
            this.sharedBufferSerializer = sharedBufferSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public NFASerializer<T> duplicate() {
            return new NFASerializer<>(eventSerializer.duplicate());
        }

        @Override
        public MigratedNFA<T> createInstance() {
            return null;
        }

        @Override
        public MigratedNFA<T> copy(MigratedNFA<T> from) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MigratedNFA<T> copy(MigratedNFA<T> from, MigratedNFA<T> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(MigratedNFA<T> record, DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MigratedNFA<T> deserialize(DataInputView source) throws IOException {
            MigrationUtils.skipSerializedStates(source);
            source.readLong();
            source.readBoolean();

            org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer =
                    sharedBufferSerializer.deserialize(source);
            Queue<ComputationState> computationStates =
                    deserializeComputationStates(sharedBuffer, eventSerializer, source);

            return new MigratedNFA<>(computationStates, sharedBuffer);
        }

        @Override
        public MigratedNFA<T> deserialize(MigratedNFA<T> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this
                    || (obj != null
                            && obj.getClass().equals(getClass())
                            && sharedBufferSerializer.equals(
                                    ((NFASerializer) obj).sharedBufferSerializer)
                            && eventSerializer.equals(((NFASerializer) obj).eventSerializer));
        }

        @Override
        public int hashCode() {
            return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
        }

        @Override
        public MigratedNFASerializerSnapshot<T> snapshotConfiguration() {
            return new MigratedNFASerializerSnapshot<>(this);
        }
    }
}

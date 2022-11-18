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

/**
 * 从{@link state}到另一个{@link state}进行状态转换时的一组操作
 * Set of actions when doing a state transition from a {@link State} to another. */
public enum StateTransitionAction {
    TAKE, // take the current event and assign it to the current state 获取当前事件并将其分配到当前状态
    IGNORE, // ignore the current event 忽略当前事件
    PROCEED // do the state transition and keep the current event for further processing (epsilon 进行状态转换并保留当前事件以供进一步处理
    // transition)

    /*
    take：消费输入事件，存入缓存，并保持当前状态；

    ignore：忽略输入事件，不存入缓存，并保持当前状态；

    proceed：感知输入事件，转移到下一个状态，同时保留该事件给下一个状态处理。
     */
}

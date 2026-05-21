/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.longpolling;

public enum PollingResult {
    // 加入到 long polling 成功
    POLLING_SUC,
    // totalPollingNum >= 100000
    // pollingMap 中对应的 key->Topic@ConsumerGroup@QueueId 的 popRequest 超过 1024
    POLLING_FULL,
    // currentTime 超过 BornTime + PollTime （过期时间戳）
    POLLING_TIMEOUT,
    // requestHeader.getPollTime() <= 0 || longPollingService.isStopped()
    NOT_POLLING;
}

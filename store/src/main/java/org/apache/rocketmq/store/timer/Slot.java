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
package org.apache.rocketmq.store.timer;

/**
 * Represents a slot of timing wheel. Format:
 * ┌────────────┬───────────┬───────────┬───────────┬───────────┐
 * │delayed time│ first pos │ last pos  │    num    │   magic   │
 * ├────────────┼───────────┼───────────┼───────────┼───────────┤
 * │   8bytes   │   8bytes  │  8bytes   │   4bytes  │   4bytes  │
 * └────────────┴───────────┴───────────┴───────────┴───────────┘
 */
public class Slot {
    public static final short SIZE = 32;
    // slot 中所有延时消息的 delayed_time
    public final long timeMs; //delayed time
    // slot 对应在 timerlog 中的第一个延时消息
    public final long firstPos;
    // slot 对应在 timerlog 中的最后一个延时消息
    public final long lastPos;
    // slot 中的延时消息个数
    public final int num;
    public final int magic; //no use now, just keep it

    public Slot(long timeMs, long firstPos, long lastPos) {
        // slot 中所有延时消息的 delayed_time
        this.timeMs = timeMs;
        // slot 对应在 timerlog 中的第一个延时消息
        this.firstPos = firstPos;
        // slot 对应在 timerlog 中的最后一个延时消息
        this.lastPos = lastPos;
        // slot 中的延时消息个数
        this.num = 0;
        this.magic = 0;
    }

    public Slot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        this.timeMs = timeMs;
        this.firstPos = firstPos;
        this.lastPos = lastPos;
        this.num = num;
        this.magic = magic;
    }
}

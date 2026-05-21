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
package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class PopMessageResponseHeader implements CommandCustomHeader {


    @CFNotNull
    private long popTime;
    @CFNotNull
    private long invisibleTime;
    // 未到 invisibleTime 的 infight 消息存储 ReviveTopic 中，时间一到，投递到 ReviveQid
    // 所以 reviveQid 中存储的都是已到 invisibleTime 的 infight 消息，重新可见
    @CFNotNull
    private int reviveQid;
    /**
     * the rest num in queue
     */
    @CFNotNull
    private long restNum;
    // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
    // 0->queueId->startOffset
    // 从多个 queue 拉消息就对应多条记录

    private String startOffsetInfo;
    // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
    // 0->queueId-> msgQueueOffsets
    // 从多个 queue 拉消息就对应多条记录
    private String msgOffsetInfo;
    // getRetry : 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
    // 0 -> qo(QUEUE_OFFSET)queueId%queueOffset -> orderCount(表示消息被消费的次数)
    // 拉取了多少条顺序消息就对应多少记录
    private String orderCountInfo;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public long getPopTime() {
        return popTime;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public long getRestNum() {
        return restNum;
    }

    public void setRestNum(long restNum) {
        this.restNum = restNum;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public int getReviveQid() {
        return reviveQid;
    }

    public void setReviveQid(int reviveQid) {
        this.reviveQid = reviveQid;
    }

    public String getStartOffsetInfo() {
        return startOffsetInfo;
    }

    public void setStartOffsetInfo(String startOffsetInfo) {
        this.startOffsetInfo = startOffsetInfo;
    }

    public String getMsgOffsetInfo() {
        return msgOffsetInfo;
    }

    public void setMsgOffsetInfo(String msgOffsetInfo) {
        this.msgOffsetInfo = msgOffsetInfo;
    }

    public String getOrderCountInfo() {
        return orderCountInfo;
    }

    public void setOrderCountInfo(String orderCountInfo) {
        this.orderCountInfo = orderCountInfo;
    }
}

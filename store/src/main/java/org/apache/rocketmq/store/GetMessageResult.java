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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetMessageResult {
    // 消息的 SelectMappedBufferResult 封装
    private final List<SelectMappedBufferResult> messageMapedList;
    // 消息的 mappedByteBuffer 原生视图
    private final List<ByteBuffer> messageBufferList; // page cache
    // 消息索引在 consume queue 中的 index (全局)
    private final List<Long> messageQueueOffset;

    private GetMessageStatus status;
    // 下一次应该从哪里开始拉取消息，该 offset 为消息在 consume queue 中的全局 index
    private long nextBeginOffset;
    // 本次拉取的消息范围，也就是本次拉取消息所在的 consumer queue 最大的 maxOffset 与最小的 minOffset
    private long minOffset;
    private long maxOffset;
    // 所有消息的 size 总大小
    private int bufferTotalSize = 0;
    // 消息个数
    private int messageCount = 0;
    // 在内存中的消息量是总体物理内存的 40% （从 commitlog 的 maxOffsetPy 往前推 40%的总物理内存量）
    // 如果剩余的消息总量超过了 40% 的总内存，那么这部分消息可能有的在磁盘中，那么就建议从 slave 拉取
    // 防止 master 的冷读
    // 拉取消息完毕之后，计算剩余的消息总量，如果剩余的消息不在内存中，则设置为 true
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;
    private int commercialSizePerMsg = 4 * 1024;
    // 1. 开启 ColdDataFlowControl
    // 2. 非系统 consumerGroup 拉取消息
    // 3. 拉取的消息不在 page cache 中
    // 统计 result 中的 cold data 大小
    private long coldDataSum = 0L;
    // 在本次消息拉取过程中通过 SQL92 表达式过滤出去的不匹配消息个数
    private int filterMessageCount;

    public static final GetMessageResult NO_MATCH_LOGIC_QUEUE =
        new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList());

    public GetMessageResult() {
        messageMapedList = new ArrayList<>(100);
        messageBufferList = new ArrayList<>(100);
        messageQueueOffset = new ArrayList<>(100);
    }

    public GetMessageResult(int resultSize) {
        messageMapedList = new ArrayList<>(resultSize);
        messageBufferList = new ArrayList<>(resultSize);
        messageQueueOffset = new ArrayList<>(resultSize);
    }

    private GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset,
        List<SelectMappedBufferResult> messageMapedList, List<ByteBuffer> messageBufferList, List<Long> messageQueueOffset) {
        this.status = status;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messageMapedList = messageMapedList;
        this.messageBufferList = messageBufferList;
        this.messageQueueOffset = messageQueueOffset;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() /  (double)commercialSizePerMsg);
        this.messageCount++;
        // 消息索引在 consume queue 中的 index (全局)
        this.messageQueueOffset.add(queueOffset);
    }


    public void addMessage(final SelectMappedBufferResult mapedBuffer, final long queueOffset, final int batchNum) {
        addMessage(mapedBuffer, queueOffset);
        messageCount += batchNum - 1;
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    public List<Long> getMessageQueueOffset() {
        return messageQueueOffset;
    }

    public long getColdDataSum() {
        return coldDataSum;
    }

    public void setColdDataSum(long coldDataSum) {
        this.coldDataSum = coldDataSum;
    }

    public int getFilterMessageCount() {
        return filterMessageCount;
    }

    public void setFilterMessageCount(int filterMessageCount) {
        this.filterMessageCount = filterMessageCount;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize + ", messageCount=" + messageCount
            + ", filterMessageCount=" + filterMessageCount + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}

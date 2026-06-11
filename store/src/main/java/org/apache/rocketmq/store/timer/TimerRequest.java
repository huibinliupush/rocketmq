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

import org.apache.rocketmq.common.message.MessageExt;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TimerRequest {
    // 延时消息在 commitlog 中的 offset
    private final long offsetPy;
    // 延时消息大小
    private final int sizePy;
    // 到期时间戳
    private final long delayTime;
    // MAGIC_DEFAULT（初始创建时）
    private final int magic;
    // 进入 enqueuePutQueue 的时间戳
    private long enqueueTime;
    // commitlog 中存储的消息实体
    private MessageExt msg;


    //optional would be a good choice, but it relies on JDK 8
    // 等待被写入 TimerWheel
    // 在 putMessageToTimerWheel 之前被设置
    // CountDownLatch deleteLatch (delete msg 个数) 在 dequeue 的时候被设置
    private CountDownLatch latch;

    private boolean released;

    //whether the operation is successful
    private boolean succ;
    // 一个空的 ConcurrentSkipListSet，由 deQueue 方法设置
    // TimerDequeueGetMessageService 会设置要取消延时消息的 UNIQKEY（PROPERTY_TIMER_DEL_UNIQKEY）
    private Set<String> deleteList;

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic) {
        this(offsetPy, sizePy, delayTime, enqueueTime, magic, null);
    }

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic, MessageExt msg) {
        // 延时消息在 commitlog 中的 offset
        this.offsetPy = offsetPy;
        // 延时消息大小
        this.sizePy = sizePy;
        // 延时消息的 delayTime（到期时间戳）
        this.delayTime = delayTime;
        // 进入 enqueuePutQueue 的时间戳
        this.enqueueTime = enqueueTime;
        // MAGIC_DEFAULT
        this.magic = magic;
        // commitlog 中存储的消息实体
        this.msg = msg;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public int getSizePy() {
        return sizePy;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public void setMsg(MessageExt msg) {
        this.msg = msg;
    }

    public int getMagic() {
        return magic;
    }

    public Set<String> getDeleteList() {
        return deleteList;
    }

    public void setDeleteList(Set<String> deleteList) {
        this.deleteList = deleteList;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }
    public void setEnqueueTime(long enqueueTime) {
        this.enqueueTime = enqueueTime;
    }
    public void idempotentRelease() {
        idempotentRelease(true);
    }
    // 将延时消息投递到 real topic 之后 release
    public void idempotentRelease(boolean succ) {
        this.succ = succ;
        if (!released && latch != null) {
            released = true;
            latch.countDown();
        }
    }

    public boolean isSucc() {
        return succ;
    }

    @Override
    public String toString() {
        return "TimerRequest{" +
            "offsetPy=" + offsetPy +
            ", sizePy=" + sizePy +
            ", delayTime=" + delayTime +
            ", enqueueTime=" + enqueueTime +
            ", magic=" + magic +
            ", msg=" + msg +
            ", latch=" + latch +
            ", released=" + released +
            ", succ=" + succ +
            ", deleteList=" + deleteList +
            '}';
    }
}

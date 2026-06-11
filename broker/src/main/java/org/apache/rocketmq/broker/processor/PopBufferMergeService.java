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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class PopBufferMergeService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    // 1. justOffset = true (默认) 并且 check point 已经写入到 reviveTopic 那么就删除该 checkpoint
    // 2. justOffset = false,checkpoint 中的消息全部 ack, 也删除
    // MergeKey : Topic consumerGroup QueueId StartOffset PopTime BrokerName
    // broker 所有 pop check point 不区分 consumerGroup ,queue
    // 如果 pop check point 已经写入 reviveTopic 则从 buffer 删除
    // 或者 pop check point 全部 ack 从 buffer 删除
    ConcurrentHashMap<String/*mergeKey*/, PopCheckPointWrapper>
        buffer = new ConcurrentHashMap<>(1024 * 16);
    // topic@cid@queueId 对应的所有 pop check point
    // 如果 check point 被成功写入 reviveTopic 之后，那么就将它的 NextBeginOffset commit
    // check point NextBeginOffset commit 之后就从 commitOffsets 中删除
    ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> commitOffsets =
        new ConcurrentHashMap<>();
    private volatile boolean serving = true;
    // 所有的 pop check point 计数
    private AtomicInteger counter = new AtomicInteger(0);
    private int scanTimes = 0;
    private final BrokerController brokerController;
    private final PopMessageProcessor popMessageProcessor;
    private final PopMessageProcessor.QueueLockManager queueLockManager;
    private final long interval = 5;
    private final long minute5 = 5 * 60 * 1000;
    private final int countOfMinute1 = (int) (60 * 1000 / interval);
    private final int countOfSecond1 = (int) (1000 / interval);
    private final int countOfSecond30 = (int) (30 * 1000 / interval);

    private final List<Byte> batchAckIndexList = new ArrayList<>(32);
    private volatile boolean master = false;

    public PopBufferMergeService(BrokerController brokerController, PopMessageProcessor popMessageProcessor) {
        this.brokerController = brokerController;
        this.popMessageProcessor = popMessageProcessor;
        this.queueLockManager = popMessageProcessor.getQueueLockManager();
    }

    private boolean isShouldRunning() {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()) {
            return true;
        }
        this.master = brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        return this.master;
    }

    @Override
    public String getServiceName() {
        if (this.brokerController != null && this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopBufferMergeService.class.getSimpleName();
        }
        return PopBufferMergeService.class.getSimpleName();
    }

    @Override
    public void run() {
        // scan
        while (!this.isStopped()) {
            try {
                if (!isShouldRunning()) {
                    // slave
                    this.waitForRunning(interval * 200 * 5);
                    POP_LOGGER.info("Broker is {}, {}, clear all data",
                        brokerController.getMessageStoreConfig().getBrokerRole(), this.master);
                    this.buffer.clear();
                    this.commitOffsets.clear();
                    continue;
                }
                // 只有 master 才会运行
                scan();
                if (scanTimes % countOfSecond30 == 0) {
                    scanGarbage();
                }
                // interval = 5, 每 5ms 执行 scan
                this.waitForRunning(interval);

                if (!this.serving && this.buffer.size() == 0 && getOffsetTotalSize() == 0) {
                    this.serving = true;
                }
            } catch (Throwable e) {
                POP_LOGGER.error("PopBufferMergeService error", e);
                this.waitForRunning(3000);
            }
        }

        this.serving = false;
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        if (!isShouldRunning()) {
            return;
        }
        while (this.buffer.size() > 0 || getOffsetTotalSize() > 0) {
            scan();
        }
    }
    // 这里就是 popOffset 的 commit 逻辑，popOffset 始终无脑一直向前推进，保证多消费者并发拉取同一队列时，每个消费者可以拉取最新的消息
    // 比如消费者 1 拉取一批消息之后，popOffset 直接向前啊推进，这样消费者2 就能拉取到后面的消息了
    // 这里的 popOffset 特指要 pop queue 的 offset, normal topic queue 和 retry topic queue 的 popOffset 的逻辑一模一样都在这里
    // check point 中的消息 ack 之后只是保证 ack 过的消息不会重新进入 retry topic 被重试消费，并不会推进 popOffset
    // 当 check point invisibleTime 到期之后，popReviveService 会检查 check point 中的 bitmaps,将没有 ack 过的消息全部
    // 写入到 retry_consumerGroup_topic 主题中，这样超时未 ack 的消息再一次对消费者可见了
    private int scanCommitOffset() {
        // topic@cid@queueId 对应的所有 pop check point
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            PopCheckPointWrapper pointWrapper;
            // 遍历 topic@cid@queueId 对应的所有 pop check point
            while ((pointWrapper = queue.peek()) != null) {
                // 1. just offset & stored, not processed by scan
                // 2. ck is buffer(acked)
                // 3. ck is buffer(not all acked), all ak are stored and ck is stored
                if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() || isCkDone(pointWrapper)
                    || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    // 这里我们只看 just offset & stored（默认的情况）
                    // 如果 check point 被成功写入 reviveTopic 之后，那么就将它的 NextBeginOffset commit
                    if (commitOffset(pointWrapper)) {
                        // check point NextBeginOffset commit 之后就从 commitOffsets 中删除
                        queue.poll();
                    } else {
                        break;
                    }
                } else {
                    if (System.currentTimeMillis() - pointWrapper.getCk().getPopTime()
                        > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2) {
                        POP_LOGGER.warn("[PopBuffer] ck offset long time not commit, {}", pointWrapper);
                    }
                    break;
                }
            }
            final int qs = queue.size();
            count += qs;
            // 队列中剩余的未提交 pop check point 个数
            if (qs > 5000 && scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer] offset queue size too long, {}, {}",
                    entry.getKey(), qs);
            }
        }
        // 全局所有 topic,consuemrGroup,queue 中剩下的未提交的 pop check point 个数
        return count;
    }

    public long getLatestOffset(String lockKey) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(lockKey);
        if (queue == null) {
            return -1;
        }
        PopCheckPointWrapper pointWrapper = queue.get().peekLast();
        if (pointWrapper != null) {
            return pointWrapper.getNextBeginOffset();
        }
        return -1;
    }

    public long getLatestOffset(String topic, String group, int queueId) {
        return getLatestOffset(KeyBuilder.buildPollingKey(topic, group, queueId));
    }

    private void scanGarbage() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            if (entry.getKey() == null) {
                continue;
            }
            String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
            if (keyArray == null || keyArray.length != 3) {
                continue;
            }
            String topic = keyArray[0];
            String cid = keyArray[1];
            if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("[PopBuffer]remove nonexistent topic {} in buffer!", topic);
                iterator.remove();
                continue;
            }
            if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                POP_LOGGER.info("[PopBuffer]remove nonexistent subscription group {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
            if (System.currentTimeMillis() - entry.getValue().getTime() > minute5) {
                POP_LOGGER.info("[PopBuffer]remove long time not used sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
        }
    }
    // 每隔 5ms
    // 将未写入 reviveTopic 的 pop checkpoint 写入
    // 提交已写入 reviveTopic 的 pop checkpoint -> nextBeginOffset
    // 也就是说 popOffset 是一直无脑向前推进的，但是具体 queue 中的 offset 需要等到 pop check point 写入到 reviveTopic 中才可以向前推进
    // 因为如果不等到 pop check point 写入到 reviveTopic 成功就向前推进 queue offset,这样就会导致如果 broker 宕机之后，pop check point
    // 中的这批消息就永远无法得到重试的机会了，因为他们不在 reviveTopic 中，但 queue offset 已经绕过了他们，broker 重启之后，消费者将会从新的 queue offset
    // 位置拉取新的消息，旧的那批消息就永无拉取机会了
    // 所以 popOffset 和 queueCommitOffset 是两个逻辑概念，一个是为了保证 pop 消费模型语义，另一个是保证 ack message 的高可用
    private void scan() {
        long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        int countCk = 0;
        // MergeKey : Topic consumerGroup QueueId StartOffset PopTime BrokerName
        // 获取 broker 所有 pop check point 不区分 consumerGroup ,queue
        Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator = buffer.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PopCheckPointWrapper> entry = iterator.next();
            // 获取 broker 所有 pop check point 不区分 consumerGroup ,queue
            PopCheckPointWrapper pointWrapper = entry.getValue();

            // just process offset(already stored at pull thread), or buffer ck(not stored and ack finish)
            // 如果 pop check point 已经写入 reviveTopic 则从 buffer 删除
            // 或者 pop check point 全部 ack 从 buffer 删除
            if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() || isCkDone(pointWrapper)
                || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("[PopBuffer]ck done, {}", pointWrapper);
                }
                // 1. justOffset = true (默认) 并且 check point 已经写入到 reviveTopic 那么就删除该 checkpoint
                // 2. justOffset = false,checkpoint 中的消息全部 ack, 也删除
                // 从 buffser 中删除
                iterator.remove();
                counter.decrementAndGet();
                continue;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            boolean removeCk = !this.serving;
            // ck will be timeout
            //  popCkStayBufferTimeOut = 3 * 1000
            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut()) {
                // 离 ReviveTime 距离不到 3s
                removeCk = true;
            }

            // the time stayed is too long
            // popCkStayBufferTime = 10 * 1000
            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime()) {
                // check point 在 buffer 里待的时间太长了，reviveTime 达到之后又过了 10s
                removeCk = true;
            }

            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
                POP_LOGGER.warn("[PopBuffer]ck finish fail, stay too long, {}", pointWrapper);
            }

            // double check
            if (isCkDone(pointWrapper)) {
                // check point 全部 ack
                continue;
            } else if (pointWrapper.isJustOffset()) { // true (默认)
                // just offset should be in store.
                // check point 在写入 reviveTopic 之后，会填充 ReviveQueueOffset
                // 如果没有填充说明还未写入到 reviveTopic
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    // 重新写入
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }
                continue;
            } else if (removeCk) {
                // put buffer ak to store
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }

                if (!pointWrapper.isCkStored()) {
                    continue;
                }
                // false
                if (brokerController.getBrokerConfig().isEnablePopBatchAck()) {
                    List<Byte> indexList = this.batchAckIndexList;
                    try {
                        for (byte i = 0; i < point.getNum(); i++) {
                            // reput buffer ak to store
                            if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                                && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                                indexList.add(i);
                            }
                        }
                        if (indexList.size() > 0) {
                            putBatchAckToStore(pointWrapper, indexList, count);
                        }
                    } finally {
                        indexList.clear();
                    }
                } else {
                    for (byte i = 0; i < point.getNum(); i++) {
                        // enablePopBufferMerge = false
                        // true : 则 popCheckPoint 保存在 PopBufferMergeService 中， ack message 的时候直接修改 PopBufferMergeService 中 check point 的 bitsmap
                        // false： 则 popCheckPoint 写入到 reviveTopic with CK_TAG , ack message 也是写入到 reviveTopic with AK_TAG
                        // reput buffer ak to store
                        // 消息已经 ack 但未写入 reviveTopic with ACK_TAG
                        if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                            && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                            // 处理 enablePopBufferMerge 的情况
                            // 将对应消息写入 reviveTopic with ACK_TAG
                            // ack 消息
                            putAckToStore(pointWrapper, i, count);
                        }
                    }
                }
                // justOffset = false 的情况下消息全部已经 ack 并且写入 reviveTopic with ACK_TAG
                // justOffset = true 的情况下 check point 是否写入 reviveTopic
                if (isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("[PopBuffer]ck finish, {}", pointWrapper);
                    }
                    // checkpoint 完成写入 reviveTopic ，从 buffer 中删除
                    iterator.remove();
                    counter.decrementAndGet();
                }
            }
        }
        // 提交 check point 中的 offset
        // 这里就是 popOffset 的 commit 逻辑，popOffset 始终无脑一直向前推进，保证多消费者并发拉取同一队列时，每个消费者可以拉取最新的消息
        // 比如消费者 1 拉取一批消息之后，popOffset 直接向前啊推进，这样消费者2 就能拉取到后面的消息了
        // 这里的 popOffset 特指要 pop queue 的 offset, normal topic queue 和 retry topic queue 的 popOffset 的逻辑一模一样都在这里
        // check point 中的消息 ack 之后只是保证 ack 过的消息不会重新进入 retry topic 被重试消费，并不会推进 popOffset
        // 当 check point invisibleTime 到期之后，popReviveService 会检查 check point 中的 bitmaps,将没有 ack 过的消息全部
        // 写入到 retry_consumerGroup_topic 主题中，这样超时未 ack 的消息再一次对消费者可见了

        // 也就是说 popOffset 是一直无脑向前推进的，但是具体 queue 中的 offset 需要等到 pop check point 写入到 reviveTopic 中才可以向前推进
        // 因为如果不等到 pop check point 写入到 reviveTopic 成功就向前推进 queue offset,这样就会导致如果 broker 宕机之后，pop check point
        // 中的这批消息就永远无法得到重试的机会了，因为他们不在 reviveTopic 中，但 queue offset 已经绕过了他们，broker 重启之后，消费者将会从新的 queue offset
        // 位置拉取新的消息，旧的那批消息就永无拉取机会了
        // 所以 popOffset 和 queueCommitOffset 是两个逻辑概念，一个是为了保证 pop 消费模型语义，另一个是保证 ack message 的高可用
        int offsetBufferSize = scanCommitOffset();

        long eclipse = System.currentTimeMillis() - startTime;
        if (eclipse > brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, " +
                    "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            this.serving = false;
        } else {
            if (scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, " +
                        "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                    eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            }
        }
        PopMetricsManager.recordPopBufferScanTimeConsume(eclipse);
        scanTimes++;

        if (scanTimes >= countOfMinute1) {
            counter.set(this.buffer.size());
            scanTimes = 0;
        }
    }

    public int getOffsetTotalSize() {
        int count = 0;
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            count += queue.size();
        }
        return count;
    }

    public int getBufferedCKSize() {
        return this.counter.get();
    }

    private void markBitCAS(AtomicInteger setBits, int index) {
        while (true) {
            // 一共 32 为 bit, 也就是说 pop 最多一次性只能 pop 32 个消息
            // 因为这里用 int 来记录消息的 ack 情况
            int bits = setBits.get();
            // bits 中的第 index 位 bit 已经是 1 了 （已经 ack 了）
            if (DataConverter.getBit(bits, index)) {
                break;
            }
            // 第 index 位设置为 1 ， 表示该 message 已经 ack
            int newBits = DataConverter.setBit(bits, index, true);
            if (setBits.compareAndSet(bits, newBits)) {
                break;
            }
        }
    }

    private boolean commitOffset(final PopCheckPointWrapper wrapper) {
        if (wrapper.getNextBeginOffset() < 0) {
            return true;
        }

        final PopCheckPoint popCheckPoint = wrapper.getCk();
        final String lockKey = wrapper.getLockKey();
        // commitOffset 的时候也需要对 queue 加锁
        if (!queueLockManager.tryLock(lockKey)) {
            return false;
        }
        try {
            final long offset = brokerController.getConsumerOffsetManager().queryOffset(popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId());
            if (wrapper.getNextBeginOffset() > offset) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("Commit offset, {}, {}", wrapper, offset);
                }
            } else {
                // maybe store offset is not correct.
                POP_LOGGER.warn("Commit offset, consumer offset less than store, {}, {}", wrapper, offset);
            }
            brokerController.getConsumerOffsetManager().commitOffset(getServiceName(),
                popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId(), wrapper.getNextBeginOffset());
        } finally {
            queueLockManager.unLock(lockKey);
        }
        return true;
    }

    private boolean putOffsetQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            queue = new QueueWithTime<>();
            QueueWithTime old = this.commitOffsets.putIfAbsent(pointWrapper.getLockKey(), queue);
            if (old != null) {
                queue = old;
            }
        }
        queue.setTime(pointWrapper.getCk().getPopTime());
        return queue.get().offer(pointWrapper);
    }

    private boolean checkQueueOk(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            return true;
        }
        // popCkOffsetMaxQueueSize = 20000 (check point 最大数量)
        return queue.get().size() < brokerController.getBrokerConfig().getPopCkOffsetMaxQueueSize();
    }

    /**
     * put to store && add to buffer.
     *
     * @param point
     * @param reviveQueueId
     * @param reviveQueueOffset
     * @param nextBeginOffset
     * @return
     */
    public boolean addCkJustOffset(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset,
        long nextBeginOffset) {
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset, true);
        // MergeKey : Topic consumerGroup QueueId StartOffset PopTime BrokerName
        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ckJustOffset. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }
        // 延时消息，将 check point 存储到 reviveTopic 中
        this.putCkToStore(pointWrapper, checkQueueOk(pointWrapper));
        // 添加到 commitOffsets 集合中
        // 集合存储了 topic@cid@queueId 对应的所有 pop check point
        putOffsetQueue(pointWrapper);
        // broker 中所有 popCheckPoint
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
        return true;
    }

    public void addCkMock(String group, String topic, int queueId, long startOffset, long invisibleTime,
        long popTime, int reviveQueueId, long nextBeginOffset, String brokerName) {
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 0);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(invisibleTime);
        ck.setStartOffset(startOffset);
        ck.setCId(group);
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, Long.MAX_VALUE, ck, nextBeginOffset, true);
        // 由 PopBufferMergerService run 直接提交 nextBeginOffset
        pointWrapper.setCkStored(true);

        putOffsetQueue(pointWrapper);
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, mocked, {}", pointWrapper);
        }
    }

    public boolean addCk(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        // key: point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt()
        // enablePopBufferMerge = false
        // true : 则 popCheckPoint 保存在 PopBufferMergeService 中， ack message 的时候直接修改 PopBufferMergeService 中 check point 的 bitsmap
        // false： 则 popCheckPoint 写入到 reviveTopic with CK_TAG , ack message 也是写入到 reviveTopic with AK_TAG
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.warn("[PopBuffer]add ck, timeout, {}, {}", point, now);
            }
            return false;
        }

        if (this.counter.get() > brokerController.getBrokerConfig().getPopCkMaxBufferSize()) {
            POP_LOGGER.warn("[PopBuffer]add ck, max size, {}, {}", point, this.counter.get());
            return false;
        }

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset);

        if (!checkQueueOk(pointWrapper)) {
            return false;
        }

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ck. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck, {}", pointWrapper);
        }
        return true;
    }

    public boolean addAk(int reviveQid, AckMsg ackMsg) {
        // enablePopBufferMerge = false
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }
        try {
            // PopCheckPointWrapper 封装了一个 pop 批次消息的所有 PopCheckPoint
            PopCheckPointWrapper pointWrapper = this.buffer.get(ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + ackMsg.getBrokerName());
            if (pointWrapper == null) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, no ck, {}", reviveQid, ackMsg);
                }
                return false;
            }
            // true
            if (pointWrapper.isJustOffset()) {
                return false;
            }
            // 本批次 pop 的 check point
            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();
            // ReviveTime : popTime + invisibleTime
            // popCkStayBufferTimeOut = 3 * 1000
            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
                // 复活剩余时间 小于 3s
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }
            // popCkStayBufferTime = 10 * 1000
            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() - 1500) {
                // 超过 10 秒没有 ack
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, stay too long, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (ackMsg instanceof BatchAckMsg) {
                for (Long ackOffset : ((BatchAckMsg) ackMsg).getAckOffsetList()) {
                    int indexOfAck = point.indexOfAck(ackOffset);
                    if (indexOfAck > -1) {
                        markBitCAS(pointWrapper.getBits(), indexOfAck);
                    } else {
                        POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    }
                }
            } else {
                // msgQueueOffset
                // ackMessage 在 check point 中 queueOffsetDiff 中的位置
                int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                if (indexOfAck > -1) {
                    // 更新 check point 中的 bit map, 对应位设置为 1 表示 ack
                    markBitCAS(pointWrapper.getBits(), indexOfAck);
                } else {
                    POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    return true;
                }
            }

            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("[PopBuffer]add ack, rqId={}, {}, {}", reviveQid, pointWrapper, ackMsg);
            }

//            // check ak done
//            if (isCkDone(pointWrapper)) {
//                // cancel ck for timer
//                cancelCkTimer(pointWrapper);
//            }
            return true;
        } catch (Throwable e) {
            POP_LOGGER.error("[PopBuffer]add ack error, rqId=" + reviveQid + ", " + ackMsg, e);
        }

        return false;
    }

    public void clearOffsetQueue(String lockKey) {
        this.commitOffsets.remove(lockKey);
    }

    private void putCkToStore(final PopCheckPointWrapper pointWrapper, final boolean runInCurrent) {
        // >=0: stored , 表示 check point 已经 stored
        if (pointWrapper.getReviveQueueOffset() >= 0) {
            return;
        }
        // 延时消息，将 check point 存储到 reviveTopic 中
        MessageExtBrokerInner msgInner = popMessageProcessor.buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());

        // Indicates that ck message is storing
        pointWrapper.setReviveQueueOffset(Long.MAX_VALUE);
        // appendCkAsync = false
        if (brokerController.getBrokerConfig().isAppendCkAsync() && runInCurrent) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                // 更新 ReviveQueueOffset
                handleCkMessagePutResult(putMessageResult, pointWrapper);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ck to store fail: {}", pointWrapper, throwable);
                pointWrapper.setReviveQueueOffset(-1);
                return null;
            });
        } else {
            // 向 reviveTopic 发送消息
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleCkMessagePutResult(putMessageResult, pointWrapper);
        }
    }

    private void handleCkMessagePutResult(PutMessageResult putMessageResult, final PopCheckPointWrapper pointWrapper) {
        PopMetricsManager.incPopReviveCkPutCount(pointWrapper.getCk(), putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            pointWrapper.setReviveQueueOffset(-1);
            POP_LOGGER.error("[PopBuffer]put ck to store fail: {}, {}", pointWrapper, putMessageResult);
            return;
        }
        // 不管情况如何反正 check point 已经写入本机 revieTopic 了： FLUSH_DISK_TIMEOUT，FLUSH_SLAVE_TIMEOUT，SLAVE_NOT_AVAILABLE
        // 当 ckStore 之后，popBufferMergerService run 会提交 ck 的 nextBeginOffset
        pointWrapper.setCkStored(true);

        if (putMessageResult.isRemotePut()) {
            //No AppendMessageResult when escaping remotely
            pointWrapper.setReviveQueueOffset(0);
        } else {
            pointWrapper.setReviveQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ck to store ok: {}, {}", pointWrapper, putMessageResult);
        }
    }

    private void putAckToStore(final PopCheckPointWrapper pointWrapper, byte msgIndex, AtomicInteger count) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(point.ackOffsetByIndex(msgIndex));
        ackMsg.setStartOffset(point.getStartOffset());
        ackMsg.setConsumerGroup(point.getCId());
        ackMsg.setTopic(point.getTopic());
        ackMsg.setQueueId(point.getQueueId());
        ackMsg.setPopTime(point.getPopTime());
        ackMsg.setBrokerName(point.getBrokerName());
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}", pointWrapper, ackMsg, throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
        }
    }

    private void handleAckPutMessageResult(AckMsg ackMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, byte msgIndex) {
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
            return;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ack to store ok: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
        }
        count.incrementAndGet();
        markBitCAS(pointWrapper.getToStoreBits(), msgIndex);
    }

    private void putBatchAckToStore(final PopCheckPointWrapper pointWrapper, final List<Byte> msgIndexList,
        AtomicInteger count) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final BatchAckMsg batchAckMsg = new BatchAckMsg();

        for (Byte msgIndex : msgIndexList) {
            batchAckMsg.getAckOffsetList().add(point.ackOffsetByIndex(msgIndex));
        }
        batchAckMsg.setStartOffset(point.getStartOffset());
        batchAckMsg.setConsumerGroup(point.getCId());
        batchAckMsg.setTopic(point.getTopic());
        batchAckMsg.setQueueId(point.getQueueId());
        batchAckMsg.setPopTime(point.getPopTime());
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody(JSON.toJSONString(batchAckMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genBatchAckUniqueId(batchAckMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put batchAckMsg to store fail: {}, {}", pointWrapper, batchAckMsg, throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
        }
    }

    private void handleBatchAckPutMessageResult(BatchAckMsg batchAckMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, List<Byte> msgIndexList) {
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put batch ack to store fail: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
            return;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put batch ack to store ok: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
        }

        count.addAndGet(msgIndexList.size());
        for (Byte i : msgIndexList) {
            markBitCAS(pointWrapper.getToStoreBits(), i);
        }
    }

    private boolean cancelCkTimer(final PopCheckPointWrapper pointWrapper) {
        // not stored, no need cancel
        if (pointWrapper.getReviveQueueOffset() < 0) {
            return true;
        }
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody((pointWrapper.getReviveQueueId() + "-" + pointWrapper.getReviveQueueOffset()).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());

        msgInner.setDeliverTimeMs(point.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]PutMessageCallback cancelCheckPoint fail, {}, {}", pointWrapper, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]cancelCheckPoint, {}", pointWrapper);
        }
        return true;
    }

    private boolean isCkDone(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        for (byte i = 0; i < num; i++) {
            if (!DataConverter.getBit(pointWrapper.getBits().get(), i)) {
                return false;
            }
        }
        return true;
    }

    private boolean isCkDoneForFinish(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        int bits = pointWrapper.getBits().get() ^ pointWrapper.getToStoreBits().get();
        for (byte i = 0; i < num; i++) {
            if (DataConverter.getBit(bits, i)) {
                // 说明至少有一个消息没有 ack 或者没有写入 reviveTopic with ACK_TAG
                return false;
            }
        }
        // 说明全部消息已经 ack 并且写入 reviveTopic with ACK_TAG
        return true;
    }

    public class QueueWithTime<T> {
        private final LinkedBlockingDeque<T> queue;
        // 最近一次 popTime, 也就是 queue 中最后一个 checkPoint 的 popTIme
        private long time;

        public QueueWithTime() {
            this.queue = new LinkedBlockingDeque<>();
            this.time = System.currentTimeMillis();
        }

        public void setTime(long popTime) {
            this.time = popTime;
        }

        public long getTime() {
            return time;
        }

        public LinkedBlockingDeque<T> get() {
            return queue;
        }
    }

    public class PopCheckPointWrapper {
        private final int reviveQueueId;
        // -1: not stored, >=0: stored, Long.MAX: storing.
        // 当 check point 存储到 reviceTopic 对应的 reviveQueueId 之后
        // 写入 check point 在 reviveQueueId 中的 queueOffset
        // 0 表示 escaping remotely 。 see:org.apache.rocketmq.broker.processor.PopBufferMergeService.handleCkMessagePutResult
        private volatile long reviveQueueOffset;
        // 本批次 pop 的 check point
        private final PopCheckPoint ck;
        // bit for concurrent，记录消息的 ack 情况
        private final AtomicInteger bits;
        // bit for stored buffer ak
        // 记录消息写入 reviveTopic 的情况
        private final AtomicInteger toStoreBits;
        // 下一次 pop 的 offset
        private final long nextBeginOffset;
        // topic@consumerGroup@queueId
        private final String lockKey;
        // Topic consumerGroup QueueId StartOffset PopTime BrokerName
        private final String mergeKey;
        // true : addAK 直接返回 false : org.apache.rocketmq.broker.processor.PopBufferMergeService.addAk
        private final boolean justOffset;
        // check point 是否成功写入到 reviveTopic
        private volatile boolean ckStored = false;

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset) {
            this.reviveQueueId = reviveQueueId;
            this.reviveQueueOffset = reviveQueueOffset;
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            this.nextBeginOffset = nextBeginOffset;
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            // EnablePopBufferMerge -> justOffset = false
            this.justOffset = false;
        }

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset,
            boolean justOffset) {
            this.reviveQueueId = reviveQueueId;
            // -1
            this.reviveQueueOffset = reviveQueueOffset;
            // 本批次 pop 的 check point
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            // 下一次 pop 的 offset
            this.nextBeginOffset = nextBeginOffset;
            // topic@consumerGroup@queueId
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            // Topic consumerGroup QueueId StartOffset PopTime BrokerName
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            // true
            this.justOffset = justOffset;
        }

        public int getReviveQueueId() {
            return reviveQueueId;
        }

        public long getReviveQueueOffset() {
            return reviveQueueOffset;
        }

        public boolean isCkStored() {
            return ckStored;
        }

        public void setReviveQueueOffset(long reviveQueueOffset) {
            this.reviveQueueOffset = reviveQueueOffset;
        }

        public PopCheckPoint getCk() {
            return ck;
        }

        public AtomicInteger getBits() {
            return bits;
        }

        public AtomicInteger getToStoreBits() {
            return toStoreBits;
        }

        public long getNextBeginOffset() {
            return nextBeginOffset;
        }

        public String getLockKey() {
            return lockKey;
        }

        public String getMergeKey() {
            return mergeKey;
        }

        public boolean isJustOffset() {
            return justOffset;
        }

        public void setCkStored(boolean ckStored) {
            this.ckStored = ckStored;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CkWrap{");
            sb.append("rq=").append(reviveQueueId);
            sb.append(", rqo=").append(reviveQueueOffset);
            sb.append(", ck=").append(ck);
            sb.append(", bits=").append(bits);
            sb.append(", sBits=").append(toStoreBits);
            sb.append(", nbo=").append(nextBeginOffset);
            sb.append(", cks=").append(ckStored);
            sb.append(", jo=").append(justOffset);
            sb.append('}');
            return sb.toString();
        }
    }

}

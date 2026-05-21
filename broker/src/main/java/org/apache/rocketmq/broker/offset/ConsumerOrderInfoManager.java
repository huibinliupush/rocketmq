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
package org.apache.rocketmq.broker.offset;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
// 每隔 10s 持久化
public class ConsumerOrderInfoManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;
    // 加载自 user.home/store/config/consumerOrderInfo.json
    // topic@group -> queueId -> OrderInfo
    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    private transient ConsumerOrderInfoLockManager consumerOrderInfoLockManager;
    private transient BrokerController brokerController;

    public ConsumerOrderInfoManager() {
    }

    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.consumerOrderInfoLockManager = new ConsumerOrderInfoLockManager(brokerController);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    protected static String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    protected static String[] decodeKey(String key) {
        return key.split(TOPIC_GROUP_SEPARATOR);
    }
    // 获取第一个未 ack 消息的 invisibleTime
    // 只要 invisibleTime 一到则 notifyMessageArrive
    private void updateLockFreeTimestamp(String topic, String group, int queueId, OrderInfo orderInfo) {
        if (consumerOrderInfoLockManager != null) {
            consumerOrderInfoLockManager.updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        }
    }

    /**
     * update the message list received
     *
     * @param isRetry is retry topic or not
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param popTime the time of pop message
     * @param invisibleTime invisible time
     * @param msgQueueOffsetList the queue offsets of messages
     * @param orderInfoBuilder will append order info to this builder
     */
    public void update(String attemptId, boolean isRetry, String topic, String group, int queueId, long popTime, long invisibleTime,
        List<Long> msgQueueOffsetList, StringBuilder orderInfoBuilder) {
        // topic@group
        String key = buildKey(topic, group);
        // 比直接使用 computeIfAbsent 性能高
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo != null) {
            OrderInfo newOrderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
            // 填充 offsetConsumedCount
            // calculate message consumed count of each message, and put nonzero value into offsetConsumedCount
            newOrderInfo.mergeOffsetConsumedCount(orderInfo.attemptId, orderInfo.offsetList, orderInfo.offsetConsumedCount);

            orderInfo = newOrderInfo;
        } else {
            // 第一次拉取到 queueId 中的顺序消息
            orderInfo = new OrderInfo(attemptId, popTime, invisibleTime, msgQueueOffsetList, System.currentTimeMillis(), 0);
        }
        qs.put(queueId, orderInfo);
        // key 是消息在 consume queue 中的 index, value 为该消息被消费的次数
        Map<Long, Integer> offsetConsumedCount = orderInfo.offsetConsumedCount;
        int minConsumedTimes = Integer.MAX_VALUE;
        if (offsetConsumedCount != null) {
            Set<Long> offsetSet = offsetConsumedCount.keySet();
            for (Long offset : offsetSet) {
                Integer consumedTimes = offsetConsumedCount.getOrDefault(offset, 0);
                // getRetry : 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
                // 0 -> qo(QUEUE_OFFSET)queueId%queueOffset -> orderCount(表示消息被消费的次数)
                // 拉取了多少条顺序消息就对应多少记录 ; 分割
                ExtraInfoUtil.buildQueueOffsetOrderCountInfo(orderInfoBuilder, topic, queueId, offset, consumedTimes);
                minConsumedTimes = Math.min(minConsumedTimes, consumedTimes);
            }

            if (offsetConsumedCount.size() != orderInfo.offsetList.size()) {
                // offsetConsumedCount only save messages which consumed count is greater than 0
                // if size not equal, means there are some new messages
                minConsumedTimes = 0;
            }
        } else {
            minConsumedTimes = 0;
        }

        // for compatibility
        // the old pop sdk use queueId to get consumedTimes from orderCountInfo
        // getRetry : 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
        // 0 -> queueId ——> orderCount(表示被消费的次数)
        ExtraInfoUtil.buildQueueIdOrderCountInfo(orderInfoBuilder, topic, queueId, minConsumedTimes);
        // 获取 orderInfo 中第一个还未 ack 的消息的 invisibleTime, 添加延时任务，当 invisibleTime 达到的时候
        // 通知 popRequest 重新拉取
        // 获取第一个未 ack 消息的 invisibleTime
        // 只要 invisibleTime 一到则 notifyMessageArrive
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }
    // 如果 inflight 消息全部 ack 则允许 pop
    // pop 出来的这批消息没有全部 ack, 但是这些未 ack 消息的 invisibleTime 必须全部到达，则允许 pop
    // 只要有一个未 ack 并且它的 invisibleTime 未到则不允许 pop
    public boolean checkBlock(String attemptId, String topic, String group, int queueId, long invisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }

        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo == null) {
            // FIFO 消息还未开始拉取
            return false;
        }
        return orderInfo.needBlock(attemptId, invisibleTime);
    }

    public void clearBlock(String topic, String group, int queueId) {
        table.computeIfPresent(buildKey(topic, group), (key, val) -> {
            val.remove(queueId);
            return val;
        });
    }

    /**
     * mark message is consumed finished. return the consumer offset
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @return -1 : illegal, -2 : no need commit, >= 0 : commit
     */
    public long commitAndNext(String topic, String group, int queueId, long queueOffset, long popTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return queueOffset + 1;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, queueOffset, orderInfo);
            return queueOffset + 1;
        }

        List<Long> o = orderInfo.offsetList;
        if (o == null || o.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }

        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, offset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return -2;
        }

        Long first = o.get(0);
        int i = 0, size = o.size();
        // queue 中 infight FIFO 消息数量
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first;
            } else {
                temp = first + o.get(i);
            }
            // 获取 ack 消息在 orderinfo 中的 index
            if (queueOffset == temp) {
                break;
            }
        }
        // not found
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, queueOffset, orderInfo);
            return -1;
        }
        //set bit
        // 在消息的对应位置上标记 ack
        orderInfo.setCommitOffsetBit(orderInfo.commitOffsetBit | (1L << i));
        // 队列中所有的 pop 顺序消息都已经 ack
        // 那就继续从最后一个 inflight 消息的下一个开始 pop
        // 从第一个没有 ack 的 inflight 消息开始 pop
        // 如果 ack 的是第二个消息，那么这里获取的仍然是第一个消息的 offset
        long nextOffset = orderInfo.getNextOffset();
        // 获取第一个未 ack 消息的 invisibleTime
        // 只要 invisibleTime 一到则 notifyMessageArrive
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
        return nextOffset;
    }

    /**
     * update next visible time of this message
     *
     * @param topic topic
     * @param group group
     * @param queueId queue id of message
     * @param queueOffset queue offset of message
     * @param nextVisibleTime nex visible time
     */
    public void updateNextVisibleTime(String topic, String group, int queueId, long queueOffset, long popTime, long nextVisibleTime) {
        String key = buildKey(topic, group);
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            log.warn("orderInfo of queueId is null. key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("orderInfo is null, key: {}, queueOffset: {}, queueId: {}", key, queueOffset, queueId);
            return;
        }
        if (popTime != orderInfo.popTime) {
            log.warn("popTime is not equal to orderInfo saved. key: {}, queueOffset: {}, orderInfo: {}, popTime: {}", key, queueOffset, orderInfo, popTime);
            return;
        }

        orderInfo.updateOffsetNextVisibleTime(queueOffset, nextVisibleTime);
        updateLockFreeTimestamp(topic, group, queueId, orderInfo);
    }

    protected void autoClean() {
        if (brokerController == null) {
            return;
        }
        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            String topicAtGroup = entry.getKey();
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = entry.getValue();
            String[] arrays = decodeKey(topicAtGroup);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];

            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig == null) {
                iterator.remove();
                log.info("Topic not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (!this.brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = qs.entrySet().iterator();
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();

                if (qsEntry.getKey() >= topicConfig.getReadQueueNums()) {
                    qsIterator.remove();
                    log.info("Queue not exist, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }

                if (System.currentTimeMillis() - qsEntry.getValue().getLastConsumeTimestamp() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                }
            }
        }
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
                if (this.consumerOrderInfoLockManager != null) {
                    this.consumerOrderInfoLockManager.recover(this.table);
                }
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean();
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public void shutdown() {
        if (this.consumerOrderInfoLockManager != null) {
            this.consumerOrderInfoLockManager.shutdown();
        }
    }

    @VisibleForTesting
    protected ConsumerOrderInfoLockManager getConsumerOrderInfoLockManager() {
        return consumerOrderInfoLockManager;
    }

    public static class OrderInfo {
        private long popTime;
        /**
         * the invisibleTime when pop message
         * 最近一次 pop 指定的 InvisibleTime
         */
        @JSONField(name = "i")
        private Long invisibleTime;
        /**
         * offset 消息在 consume queue 中的全局 index
         * offsetList[0] is the queue offset of message
         * offsetList[i] (i > 0) is the distance between current message and offsetList[0]
         * 本次拉取到的消息在 conusme queue 中的全局 index
         * 存储和第一个 消息 index 的增量
         */
        @JSONField(name = "o")
        private List<Long> offsetList;
        /**
         * next visible timestamp for message
         * key: message queue offset
         */
        @JSONField(name = "ot")
        private Map<Long, Long> offsetNextVisibleTime;
        /**
         * message consumed count for offset
         * key: message queue offset
         * 当 queue 中的消息被第一次拉取的时候只是创建 orderInfo,这里是空
         * 如果 orderInfo 中的消息全部 ack,那么这里仍然是空的
         * 只有当 orderInfo 中的未 ack 消息 invisibleTime 到期，通知重新拉取的时候，才会填充这里
         * 拉取的 FIFO 消息中包含重试消息 mergeOffsetConsumedCount 创建填充 offsetConsumedCount
         */
        @JSONField(name = "oc")
        private Map<Long, Integer> offsetConsumedCount;
        /**
         * last consume timestamp
         * 最近一次拉取顺序消息成功之后的时间戳
         */
        @JSONField(name = "l")
        private long lastConsumeTimestamp;
        /**
         * commit offset bit
         * 初始创建为 0 ， 用于记录 OrderInfo 中的消息是否 ack 相当于是个 bitmap
         * 消息 ack 之后在对应 bit 位置 1，消息在 bitmap 中的位置就是 offsetList 数组中的索引
         * 用于标记 queue 中的顺序消息是否ack
         */
        @JSONField(name = "cm")
        private long commitOffsetBit;
        // 客户端传递过来的 attemptId（pop 批次）
        // 下一次新的 pop 请求过来，这里会更新为新的 attemptId
        @JSONField(name = "a")
        private String attemptId;

        public OrderInfo() {
        }

        public OrderInfo(String attemptId, long popTime, long invisibleTime, List<Long> queueOffsetList, long lastConsumeTimestamp,
            long commitOffsetBit) {
            this.popTime = popTime;
            this.invisibleTime = invisibleTime;
            // 本次拉取到的消息在 conusme queue 中的全局 index
            // 存储和第一个 消息 index 的增量
            // 用于存储 queue 中已经被拉取到的顺序消息
            this.offsetList = buildOffsetList(queueOffsetList);
            // 本次拉取顺序消息成功之后的时间戳
            this.lastConsumeTimestamp = lastConsumeTimestamp;
            // 初始创建为 0，用于记录 OrderInfo 中的消息是否 ack 相当于是个 bitmap，消息 ack 之后在对应 bit 位置 1
            // 消息在 bitmap 中的位置就是 offsetList 数组中的索引
            // 用于标记 queue 中的顺序消息是否ack
            this.commitOffsetBit = commitOffsetBit;
            // 客户端传递过来的 attemptId
            // 下一次新的 pop 请求过来，这里会更新为新的 attemptId
            this.attemptId = attemptId;
        }

        public List<Long> getOffsetList() {
            return offsetList;
        }

        public void setOffsetList(List<Long> offsetList) {
            this.offsetList = offsetList;
        }

        public long getLastConsumeTimestamp() {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public long getCommitOffsetBit() {
            return commitOffsetBit;
        }

        public void setCommitOffsetBit(long commitOffsetBit) {
            this.commitOffsetBit = commitOffsetBit;
        }

        public long getPopTime() {
            return popTime;
        }

        public void setPopTime(long popTime) {
            this.popTime = popTime;
        }

        public Long getInvisibleTime() {
            return invisibleTime;
        }

        public void setInvisibleTime(Long invisibleTime) {
            this.invisibleTime = invisibleTime;
        }

        public Map<Long, Long> getOffsetNextVisibleTime() {
            return offsetNextVisibleTime;
        }

        public void setOffsetNextVisibleTime(Map<Long, Long> offsetNextVisibleTime) {
            this.offsetNextVisibleTime = offsetNextVisibleTime;
        }

        public Map<Long, Integer> getOffsetConsumedCount() {
            return offsetConsumedCount;
        }

        public void setOffsetConsumedCount(Map<Long, Integer> offsetConsumedCount) {
            this.offsetConsumedCount = offsetConsumedCount;
        }

        public String getAttemptId() {
            return attemptId;
        }

        public void setAttemptId(String attemptId) {
            this.attemptId = attemptId;
        }

        public static List<Long> buildOffsetList(List<Long> queueOffsetList) {
            List<Long> simple = new ArrayList<>();
            if (queueOffsetList.size() == 1) {
                simple.addAll(queueOffsetList);
                return simple;
            }
            Long first = queueOffsetList.get(0);
            simple.add(first);
            for (int i = 1; i < queueOffsetList.size(); i++) {
                // 存储和第一个 消息 index 的增量
                simple.add(queueOffsetList.get(i) - first);
            }
            return simple;
        }
        // 如果 inflight 消息全部 ack 则允许 pop
        // pop 出来的这批消息没有全部 ack, 但是这些未 ack 消息的 invisibleTime 必须全部到达，则允许 pop
        // 只要有一个未 ack 并且它的 invisibleTime 未到则不允许 pop
        @JSONField(serialize = false, deserialize = false)
        public boolean needBlock(String attemptId, long currentInvisibleTime) {
            // 还没有开始 pop 顺序消息，不需要 block， 直接 pop
            if (offsetList == null || offsetList.isEmpty()) {
                return false;
            }
            // 如果是同一 pop 批次（attemptId 相同），那么直接 pop 就行，属于同一个 pop 请求
            if (this.attemptId != null && this.attemptId.equals(attemptId)) {
                return false;
            }
            // 队列已经 pop 出来的顺序消息
            int num = offsetList.size();
            int i = 0;
            if (this.invisibleTime == null || this.invisibleTime <= 0) {
                // 最近一次 pop 指定的 currentInvisibleTime
                this.invisibleTime = currentInvisibleTime;
            }
            long currentTime = System.currentTimeMillis();
            for (; i < num; i++) {
                // 如果顺序消息还未 ack
                if (isNotAck(i)) {
                    // 最近一次 popTime
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        // 第一个未 ack 消息的 invisibleTime
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    // pop 出来的这批消息，只要有一个未 ack 并且它的 invisibleTime 未到则不允许 pop
                    if (currentTime < nextVisibleTime) {
                        return true;
                    }
                }
            }
            // 如果 inflight 消息全部 ack 则允许 pop
            // pop 出来的这批消息没有全部 ack, 但是这些未 ack 消息的 invisibleTime 必须全部到达，则允许 pop
            // 只要有一个未 ack 并且它的 invisibleTime 未到则不允许 pop
            return false;
        }
        // 如果消息全部 ack , 返回 System.currentTimeMillis 立即 notifyMessageArrive
        // 获取第一个没有 ack 的消息 invisibleTime（未到期）
        // 如果到期则跳过，获取下一个未 ack 的消息 invisibleTime , invisibleTime 一到 notifyMessageArrive
        // 如果全部到期，返回 System.currentTimeMillis 立即 notifyMessageArrive
        @JSONField(serialize = false, deserialize = false)
        public Long getLockFreeTimestamp() {
            if (offsetList == null || offsetList.isEmpty()) {
                return null;
            }
            // 已经拉取到的顺序消息个数
            int num = offsetList.size();
            int i = 0;
            long currentTime = System.currentTimeMillis();
            // 挨个遍历已经拉取到的顺序消息
            for (; i < num; i++) {
                // 顺序消息是否 ack
                if (isNotAck(i)) {
                    if (invisibleTime == null || invisibleTime <= 0) {
                        // 如果不存在不可见时间，那么不用锁定
                        return null;
                    }
                    // 消息可见时间
                    long nextVisibleTime = popTime + invisibleTime;
                    if (offsetNextVisibleTime != null) {
                        Long time = offsetNextVisibleTime.get(this.getQueueOffset(i));
                        if (time != null) {
                            nextVisibleTime = time;
                        }
                    }
                    // 获取第一个没有 ack 的消息 invisibleTime（未到期）
                    // 如果到期则跳过，获取下一个未 ack 的消息 invisibleTime
                    if (currentTime < nextVisibleTime) {
                        return nextVisibleTime;
                    }
                }
            }
            // 如果 queue 中被拉取的顺序全部 ack , 那么队列中剩下的消息就对消费者可见，不用锁定
            // 立即 notifyMessageArrive
            return currentTime;
        }

        @JSONField(serialize = false, deserialize = false)
        public void updateOffsetNextVisibleTime(long queueOffset, long nextVisibleTime) {
            if (this.offsetNextVisibleTime == null) {
                this.offsetNextVisibleTime = new HashMap<>();
            }
            this.offsetNextVisibleTime.put(queueOffset, nextVisibleTime);
        }

        @JSONField(serialize = false, deserialize = false)
        public long getNextOffset() {
            if (offsetList == null || offsetList.isEmpty()) {
                return -2;
            }
            int num = offsetList.size();
            int i = 0;
            // 获取第一个未 ack 的消息 index
            for (; i < num; i++) {
                if (isNotAck(i)) {
                    break;
                }
            }
            if (i == num) {
                // all ack
                // 队列中所有的 pop 顺序消息都已经 ack
                // 那就继续从最后一个 inflight 消息的下一个开始 pop
                return getQueueOffset(num - 1) + 1;
            }
            // 从第一个没有 ack 的 inflight 消息开始 pop (以达到可见时间)
            return getQueueOffset(i);
        }

        /**
         * convert the offset at the index of offsetList to queue offset
         *
         * @param offsetIndex the index of offsetList
         * @return queue offset of message
         */
        @JSONField(serialize = false, deserialize = false)
        public long getQueueOffset(int offsetIndex) {
            return getQueueOffset(this.offsetList, offsetIndex);
        }

        protected static long getQueueOffset(List<Long> offsetList, int offsetIndex) {
            if (offsetIndex == 0) {
                return offsetList.get(0);
            }
            return offsetList.get(0) + offsetList.get(offsetIndex);
        }

        @JSONField(serialize = false, deserialize = false)
        public boolean isNotAck(int offsetIndex) {
            return (commitOffsetBit & (1L << offsetIndex)) == 0;
        }

        /**
         * calculate message consumed count of each message, and put nonzero value into offsetConsumedCount
         * 填充 offsetConsumedCount
         *
         * @param prevOffsetConsumedCount the offset list of message
         */
        @JSONField(serialize = false, deserialize = false)
        public void mergeOffsetConsumedCount(String preAttemptId, List<Long> preOffsetList, Map<Long, Integer> prevOffsetConsumedCount) {
            Map<Long, Integer> offsetConsumedCount = new HashMap<>();
            if (prevOffsetConsumedCount == null) {
                prevOffsetConsumedCount = new HashMap<>();
            }
            // 如果属于同一批次的请求直接返回
            if (preAttemptId != null && preAttemptId.equals(this.attemptId)) {
                this.offsetConsumedCount = prevOffsetConsumedCount;
                return;
            }
            Set<Long> preQueueOffsetSet = new HashSet<>();
            for (int i = 0; i < preOffsetList.size(); i++) {
                preQueueOffsetSet.add(getQueueOffset(preOffsetList, i));
            }
            for (int i = 0; i < offsetList.size(); i++) {
                long queueOffset = this.getQueueOffset(i);
                if (preQueueOffsetSet.contains(queueOffset)) {
                    int count = 1;
                    Integer preCount = prevOffsetConsumedCount.get(queueOffset);
                    if (preCount != null) {
                        count = preCount + 1;
                    }
                    offsetConsumedCount.put(queueOffset, count);
                }
            }
            // 当 queue 中的消息被第一次拉取的时候只是创建 orderInfo,这里是空
            // 如果 orderInfo 中的消息全部 ack,那么这里仍然是空的
            // 只有当 orderInfo 中的未 ack 消息 invisibleTime 到期，通知重新拉取的时候，才会填充这里
            // 拉取的 FIFO 消息中包含重试消息 mergeOffsetConsumedCount 创建填充 offsetConsumedCount
            this.offsetConsumedCount = offsetConsumedCount;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("popTime", popTime)
                .add("invisibleTime", invisibleTime)
                .add("offsetList", offsetList)
                .add("offsetNextVisibleTime", offsetNextVisibleTime)
                .add("offsetConsumedCount", offsetConsumedCount)
                .add("lastConsumeTimestamp", lastConsumeTimestamp)
                .add("commitOffsetBit", commitOffsetBit)
                .add("attemptId", attemptId)
                .toString();
        }
    }
}

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
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public class PopReviveService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final int[] ckRewriteIntervalsInSeconds = new int[] { 10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200 };
    // reviveQueue
    private int queueId;
    private BrokerController brokerController;
    private String reviveTopic;
    // 最近一次 consumeReviveMessage 之后第一批 pop check point 的  ReviveTime
    private long currentReviveMessageTimestamp = -1;
    // 只有 master 节点才会是 true
    private volatile boolean shouldRunPopRevive = false;
    // inflightReviveRequestMap 保存的是在 reviveMsgFromCk 中还未处理完的 popCheckPoint
    // 正在处理的复活 PopCheckPoint 中 message 请求
    // timestamp 为发起复活请求时的时间戳
    // result 为是否处理完成（包括复活成功，复活失败的情况都处理完毕了）
    // 集合按照 PopCheckPoint 排序（StartOffset）
    private final NavigableMap<PopCheckPoint/* oldCK */, Pair<Long/* timestamp */, Boolean/* result */>> inflightReviveRequestMap = Collections.synchronizedNavigableMap(new TreeMap<>());
    // 最新提交的 reviveQueue 对应的 commit offset
    private long reviveOffset;

    public PopReviveService(BrokerController brokerController, String reviveTopic, int queueId) {
        // reviveQueue
        this.queueId = queueId;
        this.brokerController = brokerController;
        this.reviveTopic = reviveTopic;
        // consumerGroup : CID_RMQ_SYS_REVIVE_GROUP
        // 获取 reviveQueue 对应的 offset
        this.reviveOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + "PopReviveService_" + this.queueId;
        }
        return "PopReviveService_" + this.queueId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setShouldRunPopRevive(final boolean shouldRunPopRevive) {
        this.shouldRunPopRevive = shouldRunPopRevive;
    }

    public boolean isShouldRunPopRevive() {
        return shouldRunPopRevive;
    }

    private boolean reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            // 如果消息原来是 normal ， 那么这里就构建成 retry topic
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId(), brokerController.getBrokerConfig().isEnableRetryTopicV2()));
        } else {
            // 如果消息之前就是 retry topic 那么继续保持不变
            msgInner.setTopic(popCheckPoint.getTopic());
        }
        // retry topic 的消费者就是 popMessageProcessor， 每次 pop 的时候由 20% 概率先拉取 retry topic 中的消息
        // 消息原来的 body
        msgInner.setBody(messageExt.getBody());
        // retry topic（ %RETRY%consumerGroup_topic） 就只有一个队列
        msgInner.setQueueId(0);
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        // 每次复活的时候将 ReconsumeTimes + 1
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            // 消息第一次被 pop 出来的时间，不论后续消费被 revive 多少次，这里始终不变
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
        }
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        // 创建 retry topic（%RETRY%consumerGroup_topic)
        addRetryTopicIfNotExist(msgInner.getTopic(), popCheckPoint.getCId());
        // 同步向 retry topic 写入被复活的消息
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveRetryMessageCount(popCheckPoint, putMessageResult.getPutMessageStatus());
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},retry msg, ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                (System.currentTimeMillis() - popCheckPoint.getReviveTime()) / 1000, putMessageResult);
        }
        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            POP_LOGGER.error("reviveQueueId={}, revive error, msg is: {}", queueId, msgInner);
            return false;
        }
        this.brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(popCheckPoint);
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(popCheckPoint.getTopic(), 1);
        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        return true;
    }

    private void initPopRetryOffset(String topic, String consumerGroup) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topic, 0);
        if (offset < 0) {
            // 向 ConsumerOffsetManager 添加 retryTopic（%RETRY%consumerGroup_topic) consumerGroup 的 commitOffset
            this.brokerController.getConsumerOffsetManager().commitOffset("initPopRetryOffset", consumerGroup, topic,
                0, 0);
        }
    }

    public void addRetryTopicIfNotExist(String topic, String consumerGroup) {
        if (brokerController != null) {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig != null) {
                return;
            }
            topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum); // 1
            topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum);// 1
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(6);
            topicConfig.setTopicSysFlag(0);
            brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
            // 向 ConsumerOffsetManager 添加 retryTopic（%RETRY%consumerGroup_topic) consumerGroup 的 commitOffset(初始为 0)
            initPopRetryOffset(topic, consumerGroup);
        }
    }

    protected List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, true);
        if (pullResult == null) {
            return null;
        }
        // 没有拉取到新的消息，对应的 reviveQueue 没有消息
        if (reachTail(pullResult, offset)) {
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            }
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
            || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset();
    }

    // Triple<MessageExt, info, needRetry>
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getBizMessage(PopCheckPoint popCheckPoint, long offset) {
        // 从消息原来的 topic , queueId, queueOffset 中提取消息内容
        return this.brokerController.getEscapeBridge().getMessageAsync(popCheckPoint.getTopic(), offset, popCheckPoint.getQueueId(), popCheckPoint.getBrokerName(), false);
    }

    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        boolean deCompressBody) {
        GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult, deCompressBody);
                    brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                    brokerController.getBrokerStatsManager().incBrokerGetNums(topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, topic)
                        .put(LABEL_CONSUMER_GROUP, group)
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MixAll.isSysConsumerGroup(group))
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);

                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    POP_LOGGER.debug("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    POP_LOGGER.debug("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case OFFSET_OVERFLOW_ONE:
                    // no need to print WARN, because we use "offset + 1" to get the next message
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            try {
                long maxQueueOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                if (maxQueueOffset > offset) {
                    POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}",
                        topic, group, offset, maxQueueOffset);
                }
            } catch (ConsumeQueueException e) {
                POP_LOGGER.error("Failed to get max offset in queue", e);
            }
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        POP_LOGGER.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    protected void consumeReviveMessage(ConsumeReviveObj consumeReviveObj) {
        // key : point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName()
        // 存放 reviveQueue 中的 PopCheckPoint
        // 后续 ack message 可以通过 key 找到其所属的 PopCheckPoint
        HashMap<String, PopCheckPoint> map = consumeReviveObj.map;
        HashMap<String, PopCheckPoint> mockPointMap = new HashMap<>();
        long startScanTime = System.currentTimeMillis();
        // 最后一个消息的 DeliverTimeMs（ ck.getReviveTime() - PopAckConstants.ackTimeInterval）
        long endTime = 0;
        // 拉取 ReviveMessage 时最新的 reviveQueue 的 commitOffset
        long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
        // reviveOffset : 创建 popReviveService 时的 reviveOffset
        long oldOffset = Math.max(reviveOffset, consumeOffset);
        consumeReviveObj.oldOffset = oldOffset;
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
        long offset = oldOffset + 1;
        int noMsgCount = 0;
        // 第一个 pop check point（非 revive message）的到期时间
        long firstRt = 0;
        // offset self amend(修改)
        while (true) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            // 从对应的 reviveQueue 拉取消息
            List<MessageExt> messageExts = getReviveMessage(offset, queueId);
            // while (true) 里会一直不停地拉取 reviveQueue 中的消息直到队列中没有消息
            if (messageExts == null || messageExts.isEmpty()) {
                // reviveQueue 中的消息全部拉取完毕
                // 最后一个消息的 DeliverTimeMs
                long old = endTime;
                // 秒级单位
                long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getDequeueBehind();
                long commitLogDelay = brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehind();
                // move endTime
                if (endTime != 0 && System.currentTimeMillis() - endTime > 3 * PopAckConstants.SECOND && timerDelay <= 0 && commitLogDelay <= 0) {
                    endTime = System.currentTimeMillis();
                }
                POP_LOGGER.debug("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ",
                    queueId, offset, old, endTime, timerDelay, commitLogDelay);
                // ackTimeInterval = 1000
                if (endTime - firstRt > PopAckConstants.ackTimeInterval + PopAckConstants.SECOND) {
                    break;
                }
                noMsgCount++;
                // Fixme: why sleep is useful here?
                try {
                    Thread.sleep(100);
                } catch (Throwable ignore) {
                }
                if (noMsgCount * 100L > 4 * PopAckConstants.SECOND) {
                    break;
                } else {
                    continue;
                }
            } else {
                noMsgCount = 0;
            }
            // reviveScanTime = 10000
            // popReviveService 会一直的不停的通过 consumeReviveMessage 拉取对应 reviveQueue 中的消息
            // 直到将 reviveQueue 中的消息拉取完毕在做统一处理，但是也不能一直无限制的拉取，受到 reviveScanTime 的限制
            // 如果拉取时间超过了 reviveScanTime，就停止拉取
            if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout ", queueId);
                break;
            }
            for (MessageExt messageExt : messageExts) {
                // 在消费者拉取到消息之后，就会将该批次的 checkpoint 写入到 reviveTopic with CK_TAG
                // ck.getReviveTime = popTime + invisibleTime
                // ck.getReviveTime() - PopAckConstants.ackTimeInterval 时间达到，消息对这里可见
                if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                    // pop check point
                    String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                    if (point.getTopic() == null || point.getCId() == null) {
                        continue;
                    }
                    // key : point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName()
                    map.put(point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName(), point);
                    PopMetricsManager.incPopReviveCkGetCount(point, queueId);
                    point.setReviveOffset(messageExt.getQueueOffset());
                    if (firstRt == 0) {
                        // 第一个 pop check point（非 revive message）的到期时间
                        firstRt = point.getReviveTime();
                    }
                } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
                    // CK_TAG 消息与 ACK_TAG 消息是对应的，在消费者拉取到消息的之后先存入 CK_TAG 消息，延时时间 popTime + invisibleTime
                    // 消费者消费成功发送 ACK_TAG 消息，延时时间和 CK_TAG 一样 popTime + invisibleTime（注意这里是绝对时间，它们在会同一时间到期）
                    // 它们指定 reviveQueue 都是同一个
                    String raw = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(ackMsg, queueId);
                    String brokerName = StringUtils.isNotBlank(ackMsg.getBrokerName()) ?
                        ackMsg.getBrokerName() : brokerController.getBrokerConfig().getBrokerName();
                    // 获取 ack message 对应的 PopCheckPoint
                    String mergeKey = ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + brokerName;
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        // 出现这种情况的原因是消息拉取下来之后，消费者过了很久才 ack
                        // 由于 ack 的时间太久了，导致其所属的 pop check point 的 invisibleTime 到期
                        // pop check point 中未 ack 的消息已经重新投递到 retry topic 中，check point 已经从 reviveQueue 上删除
                        // 每处理完一个 check point 就会将 reviveQueue 的 commitOffset 向前推进，导致这里拉取不到
                        // 过了很久的时间在 ack check point 中的消息，这里就会找不到对应的 pop check point（已经处理完毕）
                        // enableSkipLongAwaitingAck = false
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            // 忽略本次 ack(因为要 ack 的消息已经被重新投递到 retry topic 中了)
                            continue;
                        }
                        // 这里不会执行，因为 enableSkipLongAwaitingAck = false
                        if (mockCkForAck(messageExt, ackMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        // 获取 ack message 在 check point 中 bitsmap 的位置
                        int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                        if (indexOfAck > -1) {
                            // 将对应位置设置为 1
                            point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                        } else {
                            POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                        }
                    }
                } else if (PopAckConstants.BATCH_ACK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find batch ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }

                    BatchAckMsg bAckMsg = JSON.parseObject(raw, BatchAckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(bAckMsg, queueId);
                    String brokerName = StringUtils.isNotBlank(bAckMsg.getBrokerName()) ?
                        bAckMsg.getBrokerName() : brokerController.getBrokerConfig().getBrokerName();
                    String mergeKey = bAckMsg.getTopic() + bAckMsg.getConsumerGroup() + bAckMsg.getQueueId() + bAckMsg.getStartOffset() + bAckMsg.getPopTime() + brokerName;
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        if (mockCkForAck(messageExt, bAckMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        List<Long> ackOffsetList = bAckMsg.getAckOffsetList();
                        for (Long ackOffset : ackOffsetList) {
                            int indexOfAck = point.indexOfAck(ackOffset);
                            if (indexOfAck > -1) {
                                point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                            } else {
                                POP_LOGGER.error("invalid batch ack index, {}, {}", bAckMsg, point);
                            }
                        }
                    }
                }
                // 更新 endTime 为最后一个消息的 DeliverTimeMs
                long deliverTime = messageExt.getDeliverTimeMs();
                if (deliverTime > endTime) {
                    endTime = deliverTime;
                }
            }
            // 继续拉取消息直到队列中没有消息
            offset = offset + messageExts.size();
        }
        consumeReviveObj.map.putAll(mockPointMap);
        consumeReviveObj.endTime = endTime;
    }

    private boolean mockCkForAck(MessageExt messageExt, AckMsg ackMsg, String mergeKey, HashMap<String, PopCheckPoint> mockPointMap) {
        // ack_tag 消息的已经到期了多久
        long ackWaitTime = System.currentTimeMillis() - messageExt.getDeliverTimeMs();
        // 3 分钟
        long reviveAckWaitMs = brokerController.getBrokerConfig().getReviveAckWaitMs();
        // ack_tag 消息以到期超过了 3 分钟
        if (ackWaitTime > reviveAckWaitMs) {
            // will use the reviveOffset of popCheckPoint to commit offset in mergeAndRevive
            PopCheckPoint mockPoint = createMockCkForAck(ackMsg, messageExt.getQueueOffset());
            POP_LOGGER.warn(
                    "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{}, mockCk:{}",
                    reviveAckWaitMs, mergeKey, ackMsg, mockPoint);
            mockPointMap.put(mergeKey, mockPoint);
            return true;
        }
        return false;
    }

    private PopCheckPoint createMockCkForAck(AckMsg ackMsg, long reviveOffset) {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(ackMsg.getStartOffset());
        point.setPopTime(ackMsg.getPopTime());
        point.setQueueId(ackMsg.getQueueId());
        point.setCId(ackMsg.getConsumerGroup());
        point.setTopic(ackMsg.getTopic());
        point.setNum((byte) 0);
        point.setBitMap(0);
        point.setReviveOffset(reviveOffset);
        point.setBrokerName(ackMsg.getBrokerName());
        return point;
    }

    protected void mergeAndRevive(ConsumeReviveObj consumeReviveObj) throws Throwable {
        // 按照 reviveOffset 从小到大顺序排序 PopCheckPoint
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        POP_LOGGER.info("reviveQueueId={}, ck listSize={}", queueId, sortList.size());
        if (sortList.size() != 0) {
            POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getStartOffset(),
                sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
        }
        // 开始拉取 reviveMessage 时 对应 reviveQueue 的 commitOffset
        // 从 oldOffset + 1 位置开始拉取 reviveMessage
        long newOffset = consumeReviveObj.oldOffset;
        for (PopCheckPoint popCheckPoint : sortList) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip ck process, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            // endTime : 最后一个 revive 消息（popCheckPoint）的 DeliverTimeMs（ ck.getReviveTime() - PopAckConstants.ackTimeInterval）
            // popCheckPoint.getReviveTime() ： 第一个 revive 消息的 popCheckPoint 复活时间
            if (consumeReviveObj.endTime - popCheckPoint.getReviveTime() <= (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                break;
            }

            // check normal topic, skip ck , if normal topic is not exist
            // 获取真正的 topic , 如果是 retry topic 这里要还原为 normal topic
            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId());
            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                POP_LOGGER.warn("reviveQueueId={}, can not get normal topic {}, then continue", queueId, popCheckPoint.getTopic());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }
            if (null == brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getCId())) {
                POP_LOGGER.warn("reviveQueueId={}, can not get cid {}, then continue", queueId, popCheckPoint.getCId());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }
            // inflightReviveRequestMap 保存的是在 reviveMsgFromCk 中还未处理完的 popCheckPoint
            while (inflightReviveRequestMap.size() > 3) {
                waitForRunning(100);
                Pair<Long, Boolean> pair = inflightReviveRequestMap.firstEntry().getValue();
                if (!pair.getObject2() && System.currentTimeMillis() - pair.getObject1() > 1000 * 30) {
                    // 超过 30s 还没有处理完 pop check point
                    PopCheckPoint oldCK = inflightReviveRequestMap.firstKey();
                    // 错误：这里的 pair left 是一个时间戳并不是 queueOffset,调用 rePutCk 逻辑混乱
                    rePutCK(oldCK, pair);
                    inflightReviveRequestMap.remove(oldCK);
                    POP_LOGGER.warn("stay too long, remove from reviveRequestMap, {}, {}, {}, {}", popCheckPoint.getTopic(),
                            popCheckPoint.getBrokerName(), popCheckPoint.getQueueId(), popCheckPoint.getStartOffset());
                }
            }
            // 从 popCheckPoint 中复活未ack得消息使其重新可见
            // 处理完之后 reviveQueue 的 commitOffset 推进到 popCheckPoint.reviveOffset
            reviveMsgFromCk(popCheckPoint);
            // 最后一个 popCheckPoint 在 reviveQueue 中的 offset
            newOffset = popCheckPoint.getReviveOffset();
        }
        if (newOffset > consumeReviveObj.oldOffset) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            // 提交 reviveQueue 的 offset (newOffset)
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }
        // 最后一个 popCheckPoint 在 reviveQueue 中的 offset
        reviveOffset = newOffset;
        consumeReviveObj.newOffset = newOffset;
    }
    // 从 popCheckPoint 中复活消息使其重新可见
    // 处理完之后 reviveQueue 的 commitOffset 推进到 popCheckPoint.reviveOffset
    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) {
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip retry, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }
        inflightReviveRequestMap.put(popCheckPoint, new Pair<>(System.currentTimeMillis(), false));
        // left 为需要复活消息的 queueOffset , right 为是否成功的写入到对应的 retry topic
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = new ArrayList<>(popCheckPoint.getNum());
        // 遍历 popCheckPoint 中的消息
        for (int j = 0; j < popCheckPoint.getNum(); j++) {
            // 如果消息被 ack 了，则直接跳过
            if (DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
                continue;
            }

            // retry msg
            // 要复活消息的 queueOffset
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            CompletableFuture<Pair<Long, Boolean>> future = getBizMessage(popCheckPoint, msgOffset)
                .thenApply(rst -> {
                    // 需要复活的消息
                    // 从消息原来的 topic , queueId, queueOffset 中提取消息内容
                    MessageExt message = rst.getLeft();
                    if (message == null) {
                        POP_LOGGER.info("reviveQueueId={}, can not get biz msg, topic:{}, qid:{}, offset:{}, brokerName:{}, info:{}, retry:{}, then continue",
                            queueId, popCheckPoint.getTopic(), popCheckPoint.getQueueId(), msgOffset, popCheckPoint.getBrokerName(), UtilAll.frontStringAtLeast(rst.getMiddle(), 60), rst.getRight());
                        return new Pair<>(msgOffset, !rst.getRight()); // Pair.object2 means OK or not, Triple.right value means needRetry
                    }
                    // 同步向 retry topic 写入被复活的消息，反正这里是后台线程，同步也无所谓
                    boolean result = reviveRetry(popCheckPoint, message);
                    return new Pair<>(msgOffset, result);
                });
            futureList.add(future);
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
            .whenComplete((v, e) -> {
                for (CompletableFuture<Pair<Long, Boolean>> future : futureList) {
                    Pair<Long, Boolean> pair = future.getNow(new Pair<>(0L, false));
                    if (!pair.getObject2()) {
                        // 复活失败,最多 17 次
                        rePutCK(popCheckPoint, pair);
                    }
                }
                // 如果 PopCheckPoint 内的消息全部 ack ，那么 futureList 就是空的
                if (inflightReviveRequestMap.containsKey(popCheckPoint)) {
                    // revive popCheckPoint 处理完成（包括复活成功，复活失败的情况都处理完毕了）
                    inflightReviveRequestMap.get(popCheckPoint).setObject2(true);
                }
                // 按照 StartOffset 从小到大的顺序遍历 PopCheckPoint
                for (Map.Entry<PopCheckPoint, Pair<Long, Boolean>> entry : inflightReviveRequestMap.entrySet()) {
                    PopCheckPoint oldCK = entry.getKey();
                    Pair<Long, Boolean> pair = entry.getValue();
                    // PopCheckPoint 处理完成
                    if (pair.getObject2()) {
                        // 提交 oldCK.getReviveOffset
                        brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, oldCK.getReviveOffset());
                        inflightReviveRequestMap.remove(oldCK);
                    } else {
                        // 前面的 PopCheckPoint 如果没有处理完，后面的就不用看了
                        break;
                    }
                }
            });
    }
    // 复活失败(写入 retry topic 失败),最多 17 次
    private void rePutCK(PopCheckPoint oldCK, Pair<Long, Boolean> pair) {
        int rePutTimes = oldCK.parseRePutTimes();
        // skipWhenCKRePutReachMaxTimes = false
        // if false, will still rewrite ck after max times 17
        if (rePutTimes >= ckRewriteIntervalsInSeconds.length && brokerController.getBrokerConfig().isSkipWhenCKRePutReachMaxTimes()) {
            POP_LOGGER.warn("rePut CK reach max times, drop it. {}, {}, {}, {}-{}, {}, {}, {}", oldCK.getTopic(), oldCK.getCId(),
                    oldCK.getBrokerName(), oldCK.getQueueId(), pair.getObject1(), oldCK.getPopTime(), oldCK.getInvisibleTime(), rePutTimes);
            return;
        }

        PopCheckPoint newCk = new PopCheckPoint();
        newCk.setBitMap(0);
        newCk.setNum((byte) 1);
        newCk.setPopTime(oldCK.getPopTime());
        newCk.setInvisibleTime(oldCK.getInvisibleTime());
        newCk.setStartOffset(pair.getObject1()); // 复活失败消息的 queueOffset
        newCk.setCId(oldCK.getCId());
        newCk.setTopic(oldCK.getTopic());
        newCk.setQueueId(oldCK.getQueueId());
        newCk.setBrokerName(oldCK.getBrokerName());
        newCk.addDiff(0);
        newCk.setRePutTimes(String.valueOf(rePutTimes + 1)); // always increment even if removed from reviveRequestMap
        if (oldCK.getReviveTime() <= System.currentTimeMillis()) {
            // never expect an ACK matched in the future, we just use it to rewrite CK and try to revive retry message next time
            int intervalIndex = rePutTimes >= ckRewriteIntervalsInSeconds.length ? ckRewriteIntervalsInSeconds.length - 1 : rePutTimes;
            // 好久之后了
            // 根据 rePutTimes 的次数从 ckRewriteIntervalsInSeconds 获取间隔
            // 重新设置 popCheckPoint 的 InvisibleTime
            newCk.setInvisibleTime(oldCK.getInvisibleTime() + ckRewriteIntervalsInSeconds[intervalIndex] * 1000);
        }
        // 重新投入到 reviveTopic
        MessageExtBrokerInner ckMsg = brokerController.getPopMessageProcessor().buildCkMsg(newCk, queueId);
        brokerController.getMessageStore().putMessage(ckMsg);
    }

    public long getReviveBehindMillis() throws ConsumeQueueException {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId);
        if (maxOffset - reviveOffset > 1) {
            return Math.max(0, System.currentTimeMillis() - currentReviveMessageTimestamp);
        }
        return 0;
    }

    public long getReviveBehindMessages() throws ConsumeQueueException {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long diff = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId) - reviveOffset;
        return Math.max(0, diff);
    }

    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() < brokerController.getShouldStartTime()) {
                    POP_LOGGER.info("PopReviveService Ready to run after {}", brokerController.getShouldStartTime());
                    this.waitForRunning(1000);
                    continue;
                }
                // 1000 每隔 1s 进行 revive, 消费 reviveTopic 对应 queue 上的 revive 消息
                this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }
                // reviveTopic 是一个延时主题，必须开启 TimerWheel
                if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
                    POP_LOGGER.warn("skip revive topic because timerWheelEnable is false");
                    continue;
                }

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                ConsumeReviveObj consumeReviveObj = new ConsumeReviveObj();
                // 获取 pop check point （CK_TAG）
                // 获取 ackMessage(AK_TAG),  如果有对应的 pop check point 则设置对应的 bitsmap 位为 1
                // 默认没有这一步：如果不存在 pop check point 则 mockCkForAck 添加到 consumeReviveObj
                // 除非 enableSkipLongAwaitingAck = true
                consumeReviveMessage(consumeReviveObj);

                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }
                // revive 收集到的 pop check point，其中没有 ack 的 message 写入到 retry topic 中
                mergeAndRevive(consumeReviveObj);
                // 通过 ReviveOffset 排序 PopCheckPoint
                ArrayList<PopCheckPoint> sortList = consumeReviveObj.sortList;
                long delay = 0;
                if (sortList != null && !sortList.isEmpty()) {
                    // 第一批 pop check point 的  ReviveTime 已经到了多久了
                    delay = (System.currentTimeMillis() - sortList.get(0).getReviveTime()) / 1000;
                    // 第一批 pop check point 的  ReviveTime
                    currentReviveMessageTimestamp = sortList.get(0).getReviveTime();
                    slow = 1;
                } else {
                    currentReviveMessageTimestamp = System.currentTimeMillis();
                }

                POP_LOGGER.info("reviveQueueId={}, revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                    queueId, consumeReviveObj.oldOffset, consumeReviveObj.newOffset, delay);
                // 连续多次没有拉取到 revive消息，延长间隔时间
                if (sortList == null || sortList.isEmpty()) {
                    POP_LOGGER.info("reviveQueueId={}, has no new msg, take a rest {}", queueId, slow);
                    // reviveInterval = 1000
                    // 连续多次没有拉取到 revive 消息则 revive 间隔时间翻倍
                    this.waitForRunning(slow * brokerController.getBrokerConfig().getReviveInterval());
                    // reviveMaxSlow = 3
                    if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                        slow++;
                    }
                }

            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId={}, revive error", queueId, e);
            }
        }
    }

    static class ConsumeReviveObj {
        // key : point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName()
        // 获取 pop check point （CK_TAG）
        // 获取 ackMessage(AK_TAG),  如果有对应的 pop check point 则设置对应的 bitsmap 位为 1
        // 默认没有这一步：如果不存在 pop check point 则 mockCkForAck 添加到 consumeReviveObj
        // 除非 enableSkipLongAwaitingAck = true
        HashMap<String, PopCheckPoint> map = new HashMap<>();
        // 通过 ReviveOffset 排序 PopCheckPoint
        ArrayList<PopCheckPoint> sortList;
        // 拉取 ReviveMessage 时最新的 reviveQueue 的 commitOffset
        // 创建 popReviveService 时的 reviveOffset
        // 两者之间的 max
        long oldOffset;
        // 最后一个消息的 DeliverTimeMs（ ck.getReviveTime() - PopAckConstants.ackTimeInterval）
        long endTime;
        // 最新提交的 reviveQueue 对应的 commit offset
        long newOffset;

        ArrayList<PopCheckPoint> genSortList() {
            if (sortList != null) {
                return sortList;
            }
            sortList = new ArrayList<>(map.values());
            sortList.sort((o1, o2) -> (int) (o1.getReviveOffset() - o2.getReviveOffset()));
            return sortList;
        }
    }
}

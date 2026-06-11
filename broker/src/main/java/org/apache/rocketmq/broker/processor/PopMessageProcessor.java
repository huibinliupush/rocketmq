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
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.pop.PopConsumerContext;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;

public class PopMessageProcessor implements NettyRequestProcessor {

    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final String BORN_TIME = "bornTime";

    private final BrokerController brokerController;
    // 用于计算从 retryTopic 中拉取重试消息的概率
    private final Random random = new Random(System.currentTimeMillis());
    // DEFAULT_CLUSTER_NAME rmq_sys_REVIVE_LOG_
    // 用于存储所有 pop check point 的 topic
    // 未到 invisibleTime 的 infight 消息存储 ReviveTopic 中，时间一到(未ack)，投递到 ReviveQid
    // 所以 reviveQid 中存储的都是已到 invisibleTime 的 infight 消息(未ack)，重新可见
    private final String reviveTopic;

    private final PopLongPollingService popLongPollingService;
    private final PopBufferMergeService popBufferMergeService;
    private final QueueLockManager queueLockManager;
    // 用于轮询选择 ReviveQueue
    private final AtomicLong ckMessageNumber;

    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        // DEFAULT_CLUSTER_NAME rmq_sys_REVIVE_LOG_
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(
            this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popLongPollingService = new PopLongPollingService(brokerController, this, false);
        this.queueLockManager = new QueueLockManager();
        this.popBufferMergeService = new PopBufferMergeService(this.brokerController, this);
        this.ckMessageNumber = new AtomicLong();
    }

    protected String getReviveTopic() {
        return reviveTopic;
    }

    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }

    public PopBufferMergeService getPopBufferMergeService() {
        return this.popBufferMergeService;
    }

    public QueueLockManager getQueueLockManager() {
        return queueLockManager;
    }

    public static String genAckUniqueId(AckMsg ackMsg) {
        return ackMsg.getTopic()
            + PopAckConstants.SPLIT + ackMsg.getQueueId()
            + PopAckConstants.SPLIT + ackMsg.getAckOffset()
            + PopAckConstants.SPLIT + ackMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + ackMsg.getPopTime()
            + PopAckConstants.SPLIT + ackMsg.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.ACK_TAG;
    }

    public static String genBatchAckUniqueId(BatchAckMsg batchAckMsg) {
        return batchAckMsg.getTopic()
            + PopAckConstants.SPLIT + batchAckMsg.getQueueId()
            + PopAckConstants.SPLIT + batchAckMsg.getAckOffsetList().toString()
            + PopAckConstants.SPLIT + batchAckMsg.getConsumerGroup()
            + PopAckConstants.SPLIT + batchAckMsg.getPopTime()
            + PopAckConstants.SPLIT + PopAckConstants.BATCH_ACK_TAG;
    }
    // Topic@QueueId@StartOffset@consumerGroup@PopTime@BrokerName@CK_TAG
    public static String genCkUniqueId(PopCheckPoint ck) {
        return ck.getTopic()
            + PopAckConstants.SPLIT + ck.getQueueId()
            + PopAckConstants.SPLIT + ck.getStartOffset()
            + PopAckConstants.SPLIT + ck.getCId()
            + PopAckConstants.SPLIT + ck.getPopTime()
            + PopAckConstants.SPLIT + ck.getBrokerName()
            + PopAckConstants.SPLIT + PopAckConstants.CK_TAG;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return popLongPollingService.getPollingMap();
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId) throws ConsumeQueueException {
        this.notifyLongPollingRequestIfNeed(
            topic, group, queueId, null, 0L, null, null);
    }

    public void notifyLongPollingRequestIfNeed(String topic, String group, int queueId,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap,
        Map<String, String> properties) throws ConsumeQueueException {
        // popOffset， FIFO 消息这里始终是 -1
        long popBufferOffset = this.brokerController.getPopMessageProcessor().getPopBufferMergeService().getLatestOffset(topic, group, queueId);
        // 已经提交的 offset， 最近一次的拉取位置 startOffset
        long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        // 队列中的 maxOffset
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        long offset = Math.max(popBufferOffset, consumerOffset);
        if (maxOffset > offset) {
            boolean notifySuccess = popLongPollingService.notifyMessageArriving(
                topic, -1, group, tagsCode, msgStoreTime, filterBitMap, properties);
            if (!notifySuccess) {
                // notify pop queue
                notifySuccess = popLongPollingService.notifyMessageArriving(
                    topic, queueId, group, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            this.brokerController.getNotificationProcessor().notifyMessageArriving(topic, queueId);
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("notify long polling request. topic:{}, group:{}, queueId:{}, success:{}",
                    topic, group, queueId, notifySuccess);
            }
        }
    }
    // offset 为消息在对应 queueId 中的个数（并不是 bytes 单位），而是 queue 中的第几个消息
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        popLongPollingService.notifyMessageArrivingWithRetryTopic(
            topic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }

    public void notifyMessageArriving(final String topic, final int queueId, final String cid) {
        popLongPollingService.notifyMessageArriving(
            topic, queueId, cid, false, null, 0L, null, null);
    }
    // POP模式下的消费者除了主动订阅 origin topic 之外，broker 还会为你自动订阅 retry topic
    // pop 消息的 offset 由 broker 的 popMessageProcessor 负责推进，拉一批往前推一批
    // 由于是消费者组下多个消费者并发 pop,所以需要保证其他消费者可以 pop 的接下来的消息，offset 只能一直向前推
    // 而 ack pop message 只能保证它不会被重试
    // 将未写入 reviveTopic 的 pop checkpoint 写入 reviveTopic
    // 提交已写入 reviveTopic 的 pop checkpoint -> nextBeginOffset
    // 也就是说 popOffset 是一直无脑向前推进的，但是具体 queue 中的 offset 需要等到 pop check point 写入到 reviveTopic 中才可以向前推进
    // 因为如果不等到 pop check point 写入到 reviveTopic 成功就向前推进 queue offset,这样就会导致如果 broker 宕机之后，pop check point
    // 中的这批消息就永远无法得到重试的机会了，因为他们不在 reviveTopic 中，但 queue offset 已经绕过了他们，broker 重启之后，消费者将会从新的 queue offset
    // 位置拉取新的消息，旧的那批消息就永无拉取机会了
    // 所以 popOffset 和 queueCommitOffset 是两个逻辑概念，一个是为了保证 pop 消费模型语义，另一个是保证 ack message 的高可用
    // org.apache.rocketmq.broker.processor.PopBufferMergeService.scan
    // FIFO 消息的 offset 管理有自己的一套方案（ConsumerOrderInfoManger, AckMessageProcessor）, 以上是非 FIFO 消息的 offset 管理

    // 当 ack FIFO 消息的时候才会提交 offset
    // 当队列中所有 inflight FIFO 消息都已经 ack，那么 offset 就推进到最后一个 inflight 消息的下一个开始 pop
    // 否则就推进到第一个没有 ack 的 inflight 消息开始 pop
    // 如果 ack 的是第二个消息，那么这里获取的仍然是第一个消息的 offset
    // see : org.apache.rocketmq.broker.processor.AckMessageProcessor.ackOrderly
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

        final long beginTimeMills = this.brokerController.getMessageStore().now();

        // fill bron time to properties if not exist, why we need this?
        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }

        Channel channel = ctx.channel();
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());

        final PopMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(PopMessageRequestHeader.class, true);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();

        // Pop mode only supports consumption in cluster load balancing mode
        brokerController.getConsumerManager().compensateBasicConsumerInfo(
            requestHeader.getConsumerGroup(), ConsumeType.CONSUME_POP, MessageModel.CLUSTERING);
        // false
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("receive PopMessage request command, {}", request);
        }

        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        // broker 是否可读
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        // 拉取 message 最多数量不能超过 32
        if (requestHeader.getMaxMsgNums() > 32) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }
        // timerWheelEnable = true , timerWheelEnable 必须开启, 因为 ReviveTopic 是一个延时 topic
        // 延时时间为消息 invisibleTime， 时间达到，ReviveTopic 投递 revive 消息到 revive queue，实现消息重新可见的语义
        if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("the broker[%s] pop message is forbidden because timerWheelEnable is false",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        // 消费者订阅的 topic 并不在该 broker 上
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }
        // topic 必须可读
        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] " +
                    "consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        // 消费者组并未在该 broker 上注册
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        SubscriptionData subscriptionData = null;
        ExpressionMessageFilter messageFilter = null;
        // 构建订阅过滤表达式
        // POP模式下的消费者除了订阅 origin topic 之外，broker 还会为你自动订阅 retry topic
        if (requestHeader.getExp() != null && !requestHeader.getExp().isEmpty()) {
            try {
                // origin topic 构建 filterExpression
                subscriptionData = FilterAPI.build(
                    requestHeader.getTopic(), requestHeader.getExp(), requestHeader.getExpType());
                // 消费者组内的消费者指定的订阅关系必须一致，否则会在这里被覆盖，导致某些消费者拉取不到消息（订阅关系被覆盖没了）
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // retry topic :  %RETRY%consumerGroup_topic
                // enableRetryTopicV2 = false
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                // 构建重试 topic 的订阅关系，因为这里是重试所以订阅关系为 SUB_ALL
                // 因为第一次订阅的时候，消息已经全部过滤完了，到了重试阶段的所有消息肯定是 consumer 需要的，不需要再过滤了
                SubscriptionData retrySubscriptionData = FilterAPI.build(
                    retryTopic, SubscriptionData.SUB_ALL, requestHeader.getExpType());
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);

                ConsumerFilterData consumerFilterData = null;
                // SQL92
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    // 构建过滤的条件表达式，只有 SQL92 才会创建 consumerFilterData
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getExp(),
                        requestHeader.getExpType(), System.currentTimeMillis());
                    if (consumerFilterData == null) {
                        POP_LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}",
                            requestHeader.getExp(), requestHeader.getConsumerGroup());
                        response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                        response.setRemark("parse the consumer's subscription failed");
                        return response;
                    }
                }
                // 订阅关系
                messageFilter = new ExpressionMessageFilter(
                    subscriptionData, consumerFilterData, brokerController.getConsumerFilterManager());
            } catch (Exception e) {
                POP_LOGGER.warn("Parse the consumer's subscription[{}] error, group: {}", requestHeader.getExp(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else { // 订阅所有
            try {
                // origin topic
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                // retry topic :  %RETRY%consumerGroup_topic
                String retryTopic = KeyBuilder.buildPopRetryTopic(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                SubscriptionData retrySubscriptionData = FilterAPI.build(retryTopic, "*", ExpressionType.TAG);
                brokerController.getConsumerManager().compensateSubscribeData(
                    requestHeader.getConsumerGroup(), retryTopic, retrySubscriptionData);
            } catch (Exception e) {
                POP_LOGGER.warn("Build default subscription error, group: {}", requestHeader.getConsumerGroup());
            }
        }
        // 用于存放拉取到的消息
        GetMessageResult getMessageResult = new GetMessageResult(requestHeader.getMaxMsgNums());
        // 用于过滤订阅的消息
        ExpressionMessageFilter finalMessageFilter = messageFilter;
        // orgin topic 的 subscriptionData
        SubscriptionData finalSubscriptionData = subscriptionData;
        // https://github.com/apache/rocketmq/wiki/%5BRIP%E2%80%9073%5D-Pop-Consumption-Improvement-Based-on-RocksDB
        // popConsumerKVServiceEnable = false
        if (brokerConfig.isPopConsumerKVServiceEnable()) {

            CompletableFuture<PopConsumerContext> popAsyncFuture = brokerController.getPopConsumerService().popAsync(
                RemotingHelper.parseChannelRemoteAddr(channel), beginTimeMills, requestHeader.getInvisibleTime(),
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                requestHeader.getMaxMsgNums(), requestHeader.isOrder(),
                requestHeader.getAttemptId(), requestHeader.getInitMode(), messageFilter);

            popAsyncFuture.thenApply(result -> {
                if (result.isFound()) {
                    response.setCode(ResponseCode.SUCCESS);
                    getMessageResult.setStatus(GetMessageStatus.FOUND);
                    // recursive processing
                    if (result.getRestCount() > 0) {
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                } else {
                    POP_LOGGER.debug("Processor not found, polling request, popTime={}, restCount={}",
                        result.getPopTime(), result.getRestCount());

                    PollingResult pollingResult = popLongPollingService.polling(
                        ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);

                    if (PollingResult.POLLING_SUC == pollingResult) {
                        // recursive processing
                        if (result.getRestCount() > 0) {
                            popLongPollingService.notifyMessageArriving(
                                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                                null, 0L, null, null);
                        }
                        return null;
                    } else if (PollingResult.POLLING_FULL == pollingResult) {
                        response.setCode(ResponseCode.POLLING_FULL);
                    } else {
                        response.setCode(ResponseCode.POLLING_TIMEOUT);
                    }
                    getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
                }

                responseHeader.setPopTime(result.getPopTime());
                responseHeader.setInvisibleTime(result.getInvisibleTime());
                responseHeader.setReviveQid(
                    requestHeader.isOrder() ? KeyBuilder.POP_ORDER_REVIVE_QUEUE : 0);
                responseHeader.setRestNum(result.getRestCount());
                responseHeader.setStartOffsetInfo(result.getStartOffsetInfo());
                responseHeader.setMsgOffsetInfo(result.getMsgOffsetInfo());
                if (requestHeader.isOrder() && !result.getOrderCountInfo().isEmpty()) {
                    responseHeader.setOrderCountInfo(result.getOrderCountInfo());
                }

                response.setRemark(getMessageResult.getStatus().name());
                if (response.getCode() != ResponseCode.SUCCESS) {
                    return response;
                }

                // add message
                result.getGetMessageResultList().forEach(temp -> {
                    for (int i = 0; i < temp.getMessageMapedList().size(); i++) {
                        getMessageResult.addMessage(temp.getMessageMapedList().get(i));
                    }
                });

                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    final byte[] r = this.readGetMessageResult(getMessageResult,
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                } else {
                    final GetMessageResult tmpGetMessageResult = getMessageResult;
                    try {
                        FileRegion fileRegion = new ManyMessageTransfer(
                            response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        channel.writeAndFlush(fileRegion)
                            .addListener((ChannelFutureListener) future -> {
                                tmpGetMessageResult.release();
                                Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                    .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()))
                                    .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                    .build();
                                RemotingMetricsManager.rpcLatency.record(
                                    request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                if (!future.isSuccess()) {
                                    POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                        channel.remoteAddress(), future.cause());
                                }
                            });
                    } catch (Throwable e) {
                        POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }
                    return null;
                }
                return response;
            }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
            return null;
        }
        // https://github.com/apache/rocketmq/wiki/%5BRIP-19%5D-Server-side-rebalance,--lightweight-consumer-client-support
        // 用于计算从 retryTopic 中拉取重试消息的概率
        int randomQ = random.nextInt(100);
        // reviveTopic : DEFAULT_CLUSTER_NAME rmq_sys_REVIVE_LOG_（延时消息类型）
        // 当消息的 invisibeTime 到达时候，投递给 reviveTopic 的 reviveQid（也就是复活消息，消息重新可见）
        // 未到 invisibleTime 的 infight 消息存储 ReviveTopic 中，时间一到，投递到 ReviveQid
        // 所以 reviveQid 中存储的都是已到 invisibleTime 的 infight 消息，重新可见
        int reviveQid; // ReviveQueue
        if (requestHeader.isOrder()) {
            // 999 , 顺序消息的 reviveQueue 只有一个,但这里只是一个标识，FIFO 场景重试并不会用到 reviveTopic（它是针对非 FIFO 重试场景的）
            // FIFO 消息重试通过 ConsumerOrderInfo 进行管理
            // 当 ack FIFO 消息的时候，如果发现 reviveQid = POP_ORDER_REVIVE_QUEUE，那么就调用 ackOrderly 方法进行 FIFO ack
            // 重试只是针对第一个未 ack FIFO 消息，将它的 invisibleTime 添加到时间轮中，一到期就 notifyMessageArriving
            // FIFO 和非 FIFO 是两套体系
            reviveQid = KeyBuilder.POP_ORDER_REVIVE_QUEUE;
        } else {
            // 轮询选择 ReviveQueue
            reviveQid = (int) Math.abs(ckMessageNumber.getAndIncrement() %
                this.brokerController.getBrokerConfig().getReviveQueueNum()); // reviveQueueNum = 8
        }
        // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
        // 0->queueId->startOffset
        // 从多个 queue 拉消息就对应多条记录用 ； 分割
        StringBuilder startOffsetInfo = new StringBuilder(64);
        // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
        // 0->queueId-> msgQueueOffsets(，分割)
        // 从多个 queue 拉消息就对应多条记录用 ； 分割
        StringBuilder msgOffsetInfo = new StringBuilder(64);
        // getRetry : 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
        // 0 -> qo(QUEUE_OFFSET)queueId%queueOffset -> orderCount(表示消息被消费的次数)
        // 拉取了多少条顺序消息就对应多少记录用 ； 分割
        StringBuilder orderCountInfo = requestHeader.isOrder() ? new StringBuilder(64) : null;

        // Due to the design of the fields startOffsetInfo, msgOffsetInfo, and orderCountInfo,
        // a single POP request could only invoke the popMsgFromQueue method once
        // for either a normal topic or a retry topic's queue. Retry topics v1 and v2 are
        // considered the same type because they share the same retry flag in previous fields.
        // Therefore, needRetryV1 is designed as a subset of needRetry, and within a single request,
        // only one type of retry topic is able to call popMsgFromQueue.

        // popFromRetryProbability = 20 , 20% 的从 retryTopic 中拉取消息（重试消息）
        boolean needRetry = randomQ < brokerConfig.getPopFromRetryProbability();
        boolean needRetryV1 = false;
        // enableRetryTopicV2 = false , retrieveMessageFromPopRetryTopicV1 = true
        if (brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
            needRetryV1 = randomQ % 2 == 0;
        }
        long popTime = System.currentTimeMillis();
        // 返回值为 restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
        CompletableFuture<Long> getMessageFuture = CompletableFuture.completedFuture(0L);
        if (needRetry && !requestHeader.isOrder()) {
            // order 不会使用 reviveTopic， 重试消息也不会进入到 retry topic
            if (needRetryV1) {
                String retryTopic = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                // %RETRY%consumerGroup_topic
                // needRetry 并且是非 FIFO message
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }
        // proxy 端设置的 queueId = -1
        // see : org.apache.rocketmq.proxy.processor.ConsumerProcessor.popMessage(org.apache.rocketmq.proxy.common.ProxyContext, org.apache.rocketmq.proxy.service.route.AddressableMessageQueue, java.lang.String, java.lang.String, int, long, long, int, org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData, boolean, org.apache.rocketmq.proxy.processor.PopMessageResultFilter, java.lang.String, long)
        if (requestHeader.getQueueId() < 0) {
            // read all queue
            getMessageFuture = popMsgFromTopic(topicConfig, false, getMessageResult, requestHeader, reviveQid, channel,
                popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
        } else {
            // 明确指定 queueId
            int queueId = requestHeader.getQueueId();
            getMessageFuture = getMessageFuture.thenCompose(restNum ->
                popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), false,
                    getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, finalMessageFilter,
                    startOffsetInfo, msgOffsetInfo, orderCountInfo));
        }
        // if not full , fetch retry again
        // 如果一开始 needRetry = false 并且不是顺序消息（因为顺序消息不是你想拉就能拉的）
        // 但是现在消息还没有拉满，那么就尝试拉取 retry 消息
        if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums() && !requestHeader.isOrder()) {
            if (needRetryV1) {
                String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                getMessageFuture = popMsgFromTopic(retryTopicV1, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            } else {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                getMessageFuture = popMsgFromTopic(retryTopic, true, getMessageResult, requestHeader, reviveQid, channel,
                    popTime, finalMessageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
            }
        }

        final RemotingCommand finalResponse = response;
        // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
        getMessageFuture.thenApply(restNum -> {
            try {
                if (request.getCallbackList() != null) {
                    request.getCallbackList().forEach(CommandCallback::accept);
                    request.getCallbackList().clear();
                }
            } catch (Throwable t) {
                POP_LOGGER.error("PopProcessor execute callback error", t);
            }
            // 拉取消息成功
            if (!getMessageResult.getMessageBufferList().isEmpty()) {
                finalResponse.setCode(ResponseCode.SUCCESS);
                getMessageResult.setStatus(GetMessageStatus.FOUND);
                if (restNum > 0) {
                    // all queue pop can not notify specified queue pop, and vice versa
                    // 通知其他消费者来继续 pop 消息
                    popLongPollingService.notifyMessageArriving(
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                        null, 0L, null, null);
                }
            } else {
                // 没有拉取到消息，那么就进行 long polling
                // 将 popRequest 加入到 pollingMap 中
                PollingResult pollingResult = popLongPollingService.polling(
                    ctx, request, new PollingHeader(requestHeader), finalSubscriptionData, finalMessageFilter);
                // 加入成功
                if (PollingResult.POLLING_SUC == pollingResult) {
                    if (restNum > 0) {
                        // 如果有消息则通知其他消费者 pop 消息，long-polling request 在 pollMap 中的排序规则
                        // 1. 过期时间越近的排在最前面
                        // 2. op(全局计数COUNTER) 越小排在最前面
                        // 目的是通知其他的 long-polling request
                        popLongPollingService.notifyMessageArriving(
                            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getConsumerGroup(),
                            null, 0L, null, null);
                    }
                    // pop 流程结束
                    return null;
                } else if (PollingResult.POLLING_FULL == pollingResult) {
                    finalResponse.setCode(ResponseCode.POLLING_FULL);
                } else {
                    finalResponse.setCode(ResponseCode.POLLING_TIMEOUT);
                }
                getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            }
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(reviveQid); // 未到 invisibleTime 的 infight 消息存储 ReviveTopic 中，时间一到，投递到 ReviveQid
            responseHeader.setRestNum(restNum);
            // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
            // 0->queueId->startOffset
            // 从多个 queue 拉消息就对应多条记录
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
            // 0->queueId-> msgQueueOffsets
            // 从多个 queue 拉消息就对应多条记录
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            if (requestHeader.isOrder() && orderCountInfo != null) {
                // getRetry : 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
                // 0 -> qo(QUEUE_OFFSET)queueId%queueOffset -> orderCount(表示消息被消费的次数)
                // 拉取了多少条顺序消息就对应多少记录
                responseHeader.setOrderCountInfo(orderCountInfo.toString());
            }
            finalResponse.setRemark(getMessageResult.getStatus().name());
            switch (finalResponse.getCode()) {
                case ResponseCode.SUCCESS:
                    // transferMsgByHeap = true
                    if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                        // 将拉取到的所有消息 ByteBuffer（page cache） 填充到一个字节数组中
                        // 消息数据先从 page cache 拷贝到 heap (r) 中，从 heap 在发送到 channel (中间涉及 heap 到 direct 拷贝)
                        final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId());
                        this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                            requestHeader.getTopic(), requestHeader.getQueueId(),
                            (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                        finalResponse.setBody(r);
                    } else {
                        // transfer messages from page cache to channel
                        final GetMessageResult tmpGetMessageResult = getMessageResult;
                        try {
                            // 不经过 pipeline 中的 encoder, 在 FileRegion 中定义好编码逻辑直接发送
                            // 封装的全是原始 mappedBuffer (page cache)
                            // 从 page cache 中直接发送
                            FileRegion fileRegion =
                                new ManyMessageTransfer(finalResponse.encodeHeader(getMessageResult.getBufferTotalSize()),
                                    getMessageResult);
                            channel.writeAndFlush(fileRegion)
                                .addListener((ChannelFutureListener) future -> {
                                    tmpGetMessageResult.release();
                                    Attributes attributes = RemotingMetricsManager.newAttributesBuilder()
                                        .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                                        .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(finalResponse.getCode()))
                                        .put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future))
                                        .build();
                                    RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributes);
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("Fail to transfer messages from page cache to {}",
                                            channel.remoteAddress(), future.cause());
                                    }
                                });
                        } catch (Throwable e) {
                            POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                            getMessageResult.release();
                        }
                        // 表示从 page cache 中发送
                        return null;
                    }
                    break;
                default:
                    return finalResponse;
            }
            // 表示从堆中发送
            return finalResponse;
        }).thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
        return null;
    }
    // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
    // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
    private CompletableFuture<Long> popMsgFromTopic(TopicConfig topicConfig, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        if (topicConfig != null) {
            // topic 中的每个队列都会拉取,直到拉取到了足够的 message
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                // 随机选取
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                getMessageFuture = getMessageFuture.thenCompose(restNum ->
                    // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
                    // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
                    popMsgFromQueue(topicConfig.getTopicName(), requestHeader.getAttemptId(), isRetry,
                        getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime, messageFilter,
                        startOffsetInfo, msgOffsetInfo, orderCountInfo));
            }
        }
        // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
        // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
        return getMessageFuture;
    }
    // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
    // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
    private CompletableFuture<Long> popMsgFromTopic(String topic, boolean isRetry, GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int reviveQid, Channel channel, long popTime,
        ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo, int randomQ, CompletableFuture<Long> getMessageFuture) {
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        return popMsgFromTopic(topicConfig, isRetry, getMessageResult, requestHeader, reviveQid, channel, popTime,
            messageFilter, startOffsetInfo, msgOffsetInfo, orderCountInfo, randomQ, getMessageFuture);
    }
    // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
    // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
    private CompletableFuture<Long> popMsgFromQueue(String topic, String attemptId, boolean isRetry,
        GetMessageResult getMessageResult,
        PopMessageRequestHeader requestHeader, int queueId, long restNum, int reviveQid,
        Channel channel, long popTime, ExpressionMessageFilter messageFilter, StringBuilder startOffsetInfo,
        StringBuilder msgOffsetInfo, StringBuilder orderCountInfo) {
        // topic@ConsumerGroup@queueId
        String lockKey =
            topic + PopAckConstants.SPLIT + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + queueId;
        boolean isOrder = requestHeader.isOrder();
        long offset;
        try {
            // 获取 queueId 的 popOffset , 只要 pop 一个 message , 那么 popOffset 就往前推，下一次 pop 就从新的 popOffset 开始
            // popOffset 保存在 popCheckPoint 中，pop 一批 message 就对应一个 popCheckPoint
            // 这样一个 topic@ConsumerGroup@queueId 就对应多个 popCheckPoint，组织在 popBufferMergeService
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                false, lockKey, false);
        } catch (ConsumeQueueException e) {
            CompletableFuture<Long> failure = new CompletableFuture<>();
            failure.completeExceptionally(e);
            return failure;
        }

        CompletableFuture<Long> future = new CompletableFuture<>();
        // 每个 lockkey: topic@consumerGroup@queueId 对应一把锁
        // 同一个 consumerGroup 下 pop message 的时候要对 queue 进行加锁
        // 不同的 consumerGroup pop 是并行的
        // ack message 也会 lock, 如果同一队列中有消息正在 ack ，那么这里就 lock 失败
        if (!queueLockManager.tryLock(lockKey)) { // CAS 锁
            try {
                // FIFO 消息不会关心剩余消息数，因为 FIFO 消息不是想拉就能拉的
                if (!requestHeader.isOrder()) {
                    // consumer queue 中还有多少个 message
                    // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
                    restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                }
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            // 获取锁失败直接返回 （consumer queue 中还有多少个 message）
            return future;
        }
        // pop message 成功之后记得释放锁
        future.whenComplete((result, throwable) -> queueLockManager.unLock(lockKey));
        // topic@consumerGroup@queueId 对应的 PopInFlightMessageNum 超过 10000 -> true
        // 默认为 false
        if (isPopShouldStop(topic, requestHeader.getConsumerGroup(), queueId)) {
            POP_LOGGER.warn("Too much msgs unacked, then stop popping. topic={}, group={}, queueId={}",
                topic, requestHeader.getConsumerGroup(), queueId);
            try {
                // consumer queue 中还有多少个 message
                // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
            } catch (ConsumeQueueException e) {
                future.completeExceptionally(e);
            }
            return future;
        }

        try {
            // topic@consumerGroup@queueId 加锁成功之后在获取一次 popOffset
            // 防止加锁期间，admin reset offset
            offset = getPopOffset(topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInitMode(),
                true, lockKey, true);

            // Current requests would calculate the total number of messages
            // waiting to be filtered for new message arrival notifications in
            // the long-polling service, need disregarding the backlog in order
            // consumption scenario. If rest message num including the blocked
            // queue accumulation would lead to frequent unnecessary wake-ups
            // of long-polling requests, resulting unnecessary CPU usage.
            // When client ack message, long-polling request would be notifications
            // by AckMessageProcessor.ackOrderly() and message will not be delayed.
            // 当前请求会计算长轮询服务中等待过滤的新消息到达通知的总数，在顺序消费场景下需要忽略积压消息。
            // 如果将阻塞队列中的消息累积也计算在内，会导致长轮询请求频繁地被不必要地唤醒（restNum > 0），从而造成不必要的 CPU 使用。当客户端确认消息时，
            // 长轮询请求会通过 `AckMessageProcessor.ackOrderly()` 进行通知，消息不会延迟发送。
            if (isOrder) {
                // 如果是顺序消费场景，如果其他 consumer 已经拉取了队列中的消息但是还没有 ack
                // 那么在这里该请求就不能拉取消息，当其他 consumer ack message 之后，会通知 long-polling service，从而发起重新拉取请求
                // 如果 inflight 消息全部 ack 则允许 pop
                // pop 出来的这批消息没有全部 ack, 但是第一个未 ack 消息的 invisibleTime 以到达，则允许 pop（FIFO消息的重试机制）
                // 只要第一个未 ack 并且它的 invisibleTime 未到则不允许 pop
                if (brokerController.getConsumerOrderInfoManager().checkBlock(
                    attemptId, topic, requestHeader.getConsumerGroup(), queueId, requestHeader.getInvisibleTime())) {
                    // should not add accumulation(max offset - consumer offset) here
                    // 顺序消费场景不能累积剩余消息，因为这里的语义是只要有剩余消息就会通知 long-polling service 来拉取
                    // 很显然，当顺序消费场景被 block 之后，即使有剩余消息也不能通知 long-polling service 来拉取
                    future.complete(restNum);
                    return future;
                }
                // 需要忽略积压消息
                // 顺序消费场景不能累积剩余消息，因为这里的语义是只要有剩余消息就会通知 long-polling service 来拉取
                // 很显然，当顺序消费场景被 block 之后，即使有剩余消息也不能通知 long-polling service 来拉取
                this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(
                    topic, requestHeader.getConsumerGroup(), queueId);
            }
            // 获取到了足够的 message 返回
            // 对于 FIFO 场景来说只要没有 block 就可以通知 long-polling service
            if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
                restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
                future.complete(restNum);
                return future;
            }
        } catch (Exception e) {
            POP_LOGGER.error("Exception in popMsgFromQueue", e);
            future.complete(restNum);
            return future;
        }
        // restNum 其实真正表示的是该 topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
        // 初始拉取 restNum = 0，每拉取一个队列，累加队列中剩余消息个数
        AtomicLong atomicRestNum = new AtomicLong(restNum);
        // popOffset
        AtomicLong atomicOffset = new AtomicLong(offset);
        // 从 consume queue 的这里开始拉取消息
        long finalOffset = offset;
        return this.brokerController.getMessageStore()
            .getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, offset,
                requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter)
            .thenCompose(result -> { // 处理拉取失败的情况
                if (result == null) {
                    return CompletableFuture.completedFuture(null);
                }
                // maybe store offset is not correct.
                if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {
                    // commit offset, because the offset is not correct
                    // If offset in store is greater than cq offset, it will cause duplicate messages,
                    // because offset in PopBuffer is not committed.
                    POP_LOGGER.warn("Pop initial offset, because store is no correct, {}, {}->{}",
                        lockKey, atomicOffset.get(), result.getNextBeginOffset());
                    // 原有的 popOffset 不正确，拉取不到消息，提交新的 popOffset（NextBeginOffset）
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                        queueId, result.getNextBeginOffset());
                    // 重新设置 popOffset
                    atomicOffset.set(result.getNextBeginOffset());
                    // 重新拉取消息
                    return this.brokerController.getMessageStore().getMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, atomicOffset.get(),
                        requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), messageFilter);
                }
                return CompletableFuture.completedFuture(result);
            }).thenApply(result -> {
                if (result == null) {
                    try {
                        // topic 中还有多少剩余消息（所有 consumer queue 剩余消息总和）
                        // 队列还剩余多少消息： 最大的消息 offset - 已经拉取到的消息个数
                        atomicRestNum.set(brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - atomicOffset.get() + atomicRestNum.get());
                    } catch (ConsumeQueueException e) {
                        POP_LOGGER.error("Failed to get max offset in queue", e);
                    }
                    return atomicRestNum.get();
                }
                // 成功拉取到了消息
                if (!result.getMessageMapedList().isEmpty()) {
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(requestHeader.getTopic(), result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), topic,
                        result.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), topic,
                        result.getBufferTotalSize());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, requestHeader.getTopic())
                        .put(LABEL_CONSUMER_GROUP, requestHeader.getConsumerGroup())
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(requestHeader.getTopic()) || MixAll.isSysConsumerGroup(requestHeader.getConsumerGroup()))
                        .put(LABEL_IS_RETRY, isRetry)
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(result.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(result.getBufferTotalSize(), attributes);

                    if (isOrder) {
                        // 如果 inflight 消息全部 ack 则允许 pop
                        // pop 出来的这批消息没有全部 ack, 但是第一个未 ack 消息的 invisibleTime 必须全部到达，则允许 pop
                        // 只要第一个未 ack 并且它的 invisibleTime 未到则不允许 pop

                        // 拉取到顺序消息之后，更新 ConsumerOrderInfoManager
                        // 创建队列的 orderInfo，一个 AttemptId 也就是一个 pop 批次对应一个 orderInfo（FIFO check point）
                        // FIFO 消息比较特殊，orderInfo 永远只会存在一个，不像非 FIFO 那样可以同时存在多个 check point
                        // 只有允许 pop FIFO  消息的时候才会更新 orderInfo
                        // 获取 orderInfo 中第一个还未 ack 的消息的 invisibleTime, 添加延时任务，当 invisibleTime 达到的时候
                        // 通知 popRequest 重新拉取

                        // 每 pop 一批新的 FIFO 消息都会创建一个新的 orderInfo（替换旧的）
                        // 在 FIFO 消费场景下，每个 topic@group@queue 只会对应一个 orderInfo，因为 FIFO 消息不是你想 pop 就能 pop 的
                        this.brokerController.getConsumerOrderInfoManager().update(requestHeader.getAttemptId(), isRetry, topic,
                            requestHeader.getConsumerGroup(),
                            queueId, popTime, requestHeader.getInvisibleTime(), result.getMessageQueueOffset(),
                            orderCountInfo);
                        // 向队列提交 offset (finalOffset)
                        // finalOffset 表示本次拉取消息的起始位置也就是上一次拉取的 NextBeginOffset
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                            requestHeader.getConsumerGroup(), topic, queueId, finalOffset);
                    } else {
                        // 非 FIFO 消息添加 CheckPoint
                        // 延时消息，将 check point 存储到 reviveTopic 中
                        // 添加到 commitOffsets 集合中
                        // 集合存储了 topic@cid@queueId 对应的所有 pop check point
                        if (!appendCheckPoint(requestHeader, topic, reviveQid, queueId, finalOffset, result, popTime, this.brokerController.getBrokerConfig().getBrokerName())) {
                            return atomicRestNum.get() + result.getMessageCount();
                        }
                    }
                    // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
                    // 0->queueId->startOffset
                    ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, queueId, finalOffset);
                    // 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2
                    // 0->queueId-> msgQueueOffsets
                    ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, queueId,
                        result.getMessageQueueOffset());
                } else if ((GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
                    || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
                    || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
                    || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus()))
                    && result.getNextBeginOffset() > -1) {
                    // 没有拉取到消息
                    if (isOrder) {
                        // 队列提交 offset -> NextBeginOffset
                        this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
                            queueId, result.getNextBeginOffset());
                    } else {
                        // 只是将 check point 添加到内存中 -> commitOffsets
                        // 不会存储到 reviveTopic
                        // 相当于是变向提交 offset -> NextBeginOffset（但未真正提交）
                        // 只不过下一次会从 NextBeginOffset 开始 pop
                        // 后续由 PopBufferMergerService run 直接提交 nextBeginOffset
                        popBufferMergeService.addCkMock(requestHeader.getConsumerGroup(), topic, queueId, finalOffset,
                            requestHeader.getInvisibleTime(), popTime, reviveQid, result.getNextBeginOffset(), brokerController.getBrokerConfig().getBrokerName());
                    }
                }

                atomicRestNum.set(result.getMaxOffset() - result.getNextBeginOffset() + atomicRestNum.get());
                String brokerName = brokerController.getBrokerConfig().getBrokerName();
                // 遍历拉取到的所有消息
                for (SelectMappedBufferResult mapedBuffer : result.getMessageMapedList()) {
                    // We should not recode buffer when popResponseReturnActualRetryTopic is true or topic is not retry topic
                    // popResponseReturnActualRetryTopic = false
                    if (brokerController.getBrokerConfig().isPopResponseReturnActualRetryTopic() || !isRetry) {
                        // 原样返回
                        getMessageResult.addMessage(mapedBuffer);
                    } else {
                        // retry topic 需要 decode，添加 PROPERTY_POP_CK 属性，还原 origin topic
                        // 从 commitlog 中的原始字节 decode 为消息实体 MessageClientExt
                        List<MessageExt> messageExtList = MessageDecoder.decodesBatch(mapedBuffer.getByteBuffer(),
                            true, false, true);
                        mapedBuffer.release();
                        for (MessageExt messageExt : messageExtList) {
                            try {
                                // startOffset popTime invisibleTime reviveQid 1( 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2) brokerName queueId msgQueueOffset
                                String ckInfo = ExtraInfoUtil.buildExtraInfo(finalOffset, popTime, requestHeader.getInvisibleTime(),
                                    reviveQid, messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                                messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);

                                // Set retry message topic to origin topic and clear message store size to recode
                                messageExt.setTopic(requestHeader.getTopic());
                                messageExt.setStoreSize(0);

                                byte[] encode = MessageDecoder.encode(messageExt, false);
                                ByteBuffer buffer = ByteBuffer.wrap(encode);
                                SelectMappedBufferResult tmpResult =
                                    new SelectMappedBufferResult(mapedBuffer.getStartOffset(), buffer, encode.length, null);
                                getMessageResult.addMessage(tmpResult);
                            } catch (Exception e) {
                                POP_LOGGER.error("Exception in recode retry message buffer, topic={}", topic, e);
                            }
                        }
                    }
                }
                this.brokerController.getPopInflightMessageCounter().incrementInFlightMessageNum(
                    topic,
                    requestHeader.getConsumerGroup(),
                    queueId,
                    result.getMessageCount()
                );
                // topic 中还剩余多少消息未拉取
                return atomicRestNum.get();
            }).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    POP_LOGGER.error("Pop message error, {}", lockKey, throwable);
                }
                queueLockManager.unLock(lockKey);
            });
    }
    // topic@consumerGroup@queueId 对应的 PopInFlightMessageNum 超过 10000 -> true
    // 默认为 false
    private boolean isPopShouldStop(String topic, String group, int queueId) {
        // enablePopMessageThreshold = false
        // popInflightMessageThreshold = 10000
        return brokerController.getBrokerConfig().isEnablePopMessageThreshold() &&
            brokerController.getPopInflightMessageCounter().getGroupPopInFlightMessageNum(topic, group, queueId) > brokerController.getBrokerConfig().getPopInflightMessageThreshold();
    }

    private long getPopOffset(String topic, String group, int queueId, int initMode, boolean init, String lockKey,
        boolean checkResetOffset) throws ConsumeQueueException {
        // 获取 consumerGroup 在 queueId 中已经提交的 offset(index)
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, queueId);
        if (offset < 0) {
            // 获取初始的消费 offset, 默认从最大的 offset 处开始消费，目的是防止冷读
            // 但是如果最小的 offset 在 page cache 中，那么就是 hot data ，不属于冷读，就从最小的这个 offset 处开始消费
            // retry topic 无论如何都是从最小的 offset 开始消费
            offset = this.getInitOffset(topic, group, queueId, initMode, init);
        }
        if (checkResetOffset) {
            // 删除 topic@consumerGroup@queueId 对应的 resetOffset 如果有的话
            Long resetOffset = resetPopOffset(topic, group, queueId);
            if (resetOffset != null) {
                // admin reset 了 offset , 所以要以 resetOffset 为准
                return resetOffset;
            }
        }
        // lockKey : topic@ConsumerGroup@queueId
        // 获取 queueId 最新的 pop offset(这一次 pop 操作就从这里开始)
        // NextBeginOffset
        long bufferOffset = this.popBufferMergeService.getLatestOffset(lockKey);
        if (bufferOffset < 0) {
            return offset;
        } else {
            return Math.max(bufferOffset, offset);
        }
    }
    // 获取初始的消费 offset, 默认从最大的 offset 处开始消费，目的是防止冷读
    // 但是如果最小的 offset 在 page cache 中，那么就是 hot data ，不属于冷读，就从最小的这个 offset 处开始消费
    public long getInitOffset(String topic, String group, int queueId, int initMode, boolean init)
        throws ConsumeQueueException {
        long offset;
        if (ConsumeInitMode.MIN == initMode || topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            // 从队列最小的 offset 开始消费
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        } else {
            // initPopOffsetByCheckMsgInMem = true
            if (this.brokerController.getBrokerConfig().isInitPopOffsetByCheckMsgInMem() &&
                this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId) <= 0 &&
                // mincore 判断消息所在 commitlog 的位置是否在 page cache 中
                this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
                // 如果 consumer queue 中 consumerOffset = 0 的 cqUnit 在 page cache 中，那么就从一开始进行消费
                // 因为都在 page cache 中不属于冷读
                offset = 0;
            } else {
                // pop last one,then commit offset.
                // 从 consumer queue 最大的 offset 处开始消费，目的是防止大量冷读
                offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
                // max & no consumer offset
                if (offset < 0) {
                    offset = 0;
                }
            }
        }
        if (init) { // whichever initMode
            // 记录该 consumerGroup 消费 topic 下 queueid 的 offset
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "getPopOffset", group, topic, queueId, offset);
        }
        return offset;
    }

    public MessageExtBrokerInner buildCkMsg(final PopCheckPoint ck, final int reviveQid) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        // DEFAULT_CLUSTER_NAME rmq_sys_REVIVE_LOG_
        // 用于存储所有 pop check point 的 topic
        msgInner.setTopic(reviveTopic);
        // pop check point
        msgInner.setBody(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(reviveQid);
        // ck
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        // ReviveTime : popTime + invisibleTime
        // ackTimeInterval = 1000
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        // messageId : Topic@QueueId@StartOffset@consumerGroup@PopTime@BrokerName@CK_TAG
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    private boolean appendCheckPoint(final PopMessageRequestHeader requestHeader,
        final String topic, final int reviveQid, final int queueId, final long offset,
        final GetMessageResult getMessageTmpResult, final long popTime, final String brokerName) {
        // add check point msg to revive log
        final PopCheckPoint ck = new PopCheckPoint();
        // 用于记录 inflight pop message 的 ack 情况
        ck.setBitMap(0);
        // 本次拉取消息的个数
        ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        // 本次消息拉取的起始 offset
        ck.setStartOffset(offset);
        // 拉取消息的 ConsumerGroup
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);
        for (Long msgQueueOffset : getMessageTmpResult.getMessageQueueOffset()) {
            // 存储本批次 pop message 的 queueOffset
            ck.addDiff((int) (msgQueueOffset - offset));
        }

        this.brokerController.getBrokerStatsManager().incBrokerCkNums(1);
        this.brokerController.getBrokerStatsManager().incGroupCkNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);
        // 默认不合并，addBufferSuc = false
        // enablePopBufferMerge = false
        // true : 则 popCheckPoint 保存在 PopBufferMergeService 中， ack message 的时候直接修改 PopBufferMergeService 中 check point 的 bitsmap
        // false： 则 popCheckPoint 写入到 reviveTopic with CK_TAG , ack message 也是写入到 reviveTopic with AK_TAG
        final boolean addBufferSuc = this.popBufferMergeService.addCk(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );

        if (addBufferSuc) {
            return true;
        }
        // 延时消息，将 check point 存储到 reviveTopic 中
        // 添加到 commitOffsets 集合中
        // 集合存储了 topic@cid@queueId 对应的所有 pop check point
        return this.popBufferMergeService.addCkJustOffset(
            ck, reviveQid, -1, getMessageTmpResult.getNextBeginOffset()
        );
    }

    private Long resetPopOffset(String topic, String group, int queueId) {
        String lockKey = topic + PopAckConstants.SPLIT + group + PopAckConstants.SPLIT + queueId;
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);
        if (resetOffset != null) {
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topic, group, queueId);
            this.getPopBufferMergeService().clearOffsetQueue(lockKey);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", group, topic, queueId, resetOffset);
        }
        return resetOffset;
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic,
        final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                // 从 page cache 到 堆
                byteBuffer.put(bb);
                // 最近拉取消息的 storeTime
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
            this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    static class TimedLock {
        private final AtomicBoolean lock;
        // 成功获取锁时的时间戳
        private volatile long lockTime;

        public TimedLock() {
            // init lock status, false means not locked
            this.lock = new AtomicBoolean(false);
            this.lockTime = System.currentTimeMillis();
        }

        public boolean tryLock() {
            boolean ret = lock.compareAndSet(false, true);
            if (ret) {
                this.lockTime = System.currentTimeMillis();
                return true;
            } else {
                return false;
            }
        }

        public void unLock() {
            lock.set(false);
        }

        public boolean isLock() {
            return lock.get();
        }

        public long getLockTime() {
            return lockTime;
        }
    }

    public class QueueLockManager extends ServiceThread {
        // 每个 lockkey: topic@consumerGroup@queueId 对应一把锁
        // 同一个 consumerGroup 下 pop message 的时候要对 queue 进行加锁
        // 不同的 consumerGroup pop 是并行的
        private final ConcurrentHashMap<String, TimedLock> expiredLocalCache = new ConcurrentHashMap<>(100000);

        public String buildLockKey(String topic, String consumerGroup, int queueId) {
            return topic + PopAckConstants.SPLIT + consumerGroup + PopAckConstants.SPLIT + queueId;
        }

        public boolean tryLock(String topic, String consumerGroup, int queueId) {
            return tryLock(buildLockKey(topic, consumerGroup, queueId));
        }
        // pop 消息的时候以及 ack 消息的时候需要加锁，commitOffset 的时候也需要
        // pop 消息的时候 lock 失败，直接跳过，获取下一个 queuelock
        // ack 消息的时候会一直 try lock 直到成功
        public boolean tryLock(String key) {
            TimedLock timedLock = ConcurrentHashMapUtils.computeIfAbsent(expiredLocalCache, key, k -> new TimedLock());
            return timedLock.tryLock();
        }

        /**
         * is not thread safe, may cause duplicate lock
         *
         * @param usedExpireMillis the expired time in millisecond
         * @return total numbers of TimedLock
         */
        public int cleanUnusedLock(final long usedExpireMillis) {
            Iterator<Entry<String, TimedLock>> iterator = expiredLocalCache.entrySet().iterator();

            int total = 0;
            while (iterator.hasNext()) {
                Entry<String, TimedLock> entry = iterator.next();

                if (System.currentTimeMillis() - entry.getValue().getLockTime() > usedExpireMillis) {
                    iterator.remove();
                    POP_LOGGER.info("Remove unused queue lock: {}, {}, {}", entry.getKey(),
                        entry.getValue().getLockTime(),
                        entry.getValue().isLock());
                }

                total++;
            }

            return total;
        }

        public void unLock(String topic, String consumerGroup, int queueId) {
            unLock(buildLockKey(topic, consumerGroup, queueId));
        }

        public void unLock(String key) {
            TimedLock timedLock = expiredLocalCache.get(key);
            if (timedLock != null) {
                timedLock.unLock();
            }
        }

        @Override
        public String getServiceName() {
            if (PopMessageProcessor.this.brokerController.getBrokerConfig().isInBrokerContainer()) {
                return PopMessageProcessor.this.brokerController.getBrokerIdentity().getIdentifier() + QueueLockManager.class.getSimpleName();
            }
            return QueueLockManager.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    this.waitForRunning(60000);
                    int count = cleanUnusedLock(60000);
                    POP_LOGGER.info("QueueLockSize={}", count);
                } catch (Exception e) {
                    PopMessageProcessor.POP_LOGGER.error("QueueLockManager run error", e);
                }
            }
        }
    }
}

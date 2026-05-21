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
package org.apache.rocketmq.proxy.processor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

public class ProducerProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    // producerProcessorExecutor , PROCESSOR_NUMBER, 10000
    private final ExecutorService executor;
    private final TopicMessageTypeValidator topicMessageTypeValidator;

    public ProducerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager, ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, int sysFlag, List<Message> messageList, long timeoutMillis) {
        CompletableFuture<List<SendResult>> future = new CompletableFuture<>();
        long beginTimestampFirst = System.currentTimeMillis();
        AddressableMessageQueue messageQueue = null;
        try {
            Message message = messageList.get(0);
            String topic = message.getTopic();
            if (isNeedCheckTopicMessageType(message)) {
                if (topicMessageTypeValidator != null) {
                    // Do not check retry or dlq topic
                    if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
                        TopicMessageType topicMessageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
                        TopicMessageType messageType = TopicMessageType.parseFromMessageProperty(message.getProperties());
                        topicMessageTypeValidator.validate(topicMessageType, messageType);
                    }
                }
            }
            // 从 topic 对应的 MessageQueueView（所有副本集下的 messageQueue）选取队列
            messageQueue = queueSelector.select(ctx,
                this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no writable queue");
            }

            for (Message msg : messageList) {
                // 为每个消息生成 messageId :  ip,pid,MessageClientIDSetter-hashcode,当前时间与月初1号的时间差值（毫秒）,COUNTER
                MessageClientIDSetter.setUniqID(msg);
            }
            SendMessageRequestHeader requestHeader = buildSendMessageRequestHeader(messageList, producerGroup, sysFlag, messageQueue.getQueueId());

            AddressableMessageQueue finalMessageQueue = messageQueue;
            future = this.serviceManager.getMessageService().sendMessage(
                ctx,
                messageQueue,
                messageList,
                requestHeader,
                timeoutMillis)
                .thenApplyAsync(sendResultList -> {
                    for (SendResult sendResult : sendResultList) {
                        int tranType = MessageSysFlag.getTransactionValue(requestHeader.getSysFlag());
                        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus()) &&
                            tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE &&
                            StringUtils.isNotBlank(sendResult.getTransactionId())) {
                            fillTransactionData(ctx, producerGroup, finalMessageQueue, sendResult, messageList);
                        }
                    }
                    return sendResultList;
                }, this.executor)  // producerProcessorExecutor , PROCESSOR_NUMBER, 10000
                    .whenComplete((result, exception) -> {
                        long endTimestamp = System.currentTimeMillis();
                        if (exception != null) {
                            this.serviceManager.getTopicRouteService().updateFaultItem(finalMessageQueue.getBrokerName(), endTimestamp - beginTimestampFirst, true, false);
                        } else {
                            this.serviceManager.getTopicRouteService().updateFaultItem(finalMessageQueue.getBrokerName(),endTimestamp - beginTimestampFirst, false, true);
                        }
                    });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor); // producerProcessorExecutor , PROCESSOR_NUMBER, 10000
    }

    public CompletableFuture<String> recallMessage(ProxyContext ctx, String topic,
                                                   String recallHandle, long timeoutMillis) {
        CompletableFuture<String> future = new CompletableFuture<>();
        try {
            if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
                TopicMessageType messageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
                topicMessageTypeValidator.validate(messageType, TopicMessageType.DELAY);
            }

            RecallMessageHandle.HandleV1 handleEntity;
            try {
                handleEntity = (RecallMessageHandle.HandleV1) RecallMessageHandle.decodeHandle(recallHandle);
            } catch (DecoderException e) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, e.getMessage());
            }
            String brokerName = handleEntity.getBrokerName();
            RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setRecallHandle(recallHandle);
            requestHeader.setBrokerName(brokerName);
            future = serviceManager.getMessageService().recallMessage(ctx, brokerName, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected void fillTransactionData(ProxyContext ctx, String producerGroup, AddressableMessageQueue messageQueue, SendResult sendResult, List<Message> messageList) {
        try {
            MessageId id;
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
            this.serviceManager.getTransactionService().addTransactionDataByBrokerName(
                ctx,
                messageQueue.getBrokerName(),
                messageList.get(0).getTopic(),
                producerGroup,
                sendResult.getQueueOffset(),
                id.getOffset(),
                sendResult.getTransactionId(),
                messageList.get(0)
            );
        } catch (Throwable t) {
            log.warn("fillTransactionData failed. messageQueue: {}, sendResult: {}", messageQueue, sendResult, t);
        }
    }

    protected SendMessageRequestHeader buildSendMessageRequestHeader(List<Message> messageList,
        String producerGroup, int sysFlag, int queueId) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Message message = messageList.get(0);

        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);// 如果没有通过 admin 创建 topic 那么就会自动创建 topic
        requestHeader.setDefaultTopicQueueNums(4);// 自动创建 topic 时指定的 QueueNums
        requestHeader.setQueueId(queueId);
        requestHeader.setSysFlag(sysFlag);
        /*
        In RocketMQ 4.0, org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader.bornTimestamp
        represents the timestamp when the message was born. In RocketMQ 5.0, the bornTimestamp of the message
        is a message attribute, that is, the timestamp when message was constructed, and there is no
        bornTimestamp in the SendMessageRequest of RocketMQ 5.0.
        Note: When using grpc sendMessage to send multiple messages, the bornTimestamp in the requestHeader
        is set to the bornTimestamp of the first message, which may not be accurate. When a bornTimestamp is
        required, the bornTimestamp of the message property should be used.
        * */
        try {
            requestHeader.setBornTimestamp(Long.parseLong(message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP)));
        } catch (Exception e) {
            log.warn("parse born time error, with value:{}", message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP));
            requestHeader.setBornTimestamp(System.currentTimeMillis());
        }
        requestHeader.setFlag(message.getFlag());
        // name1(NAME_VALUE_SEPARATOR)value2(PROPERTY_SEPARATOR)name1value2name1value2
        // NAME_VALUE_SEPARATOR = 1,PROPERTY_SEPARATOR=2
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setReconsumeTimes(0);
        if (messageList.size() > 1) {
            requestHeader.setBatch(true);
        }
        // retry topic
        if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            // PROPERTY_RECONSUME_TIME
            String reconsumeTimes = MessageAccessor.getReconsumeTime(message);
            if (reconsumeTimes != null) {
                requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_RECONSUME_TIME);
            }

            String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(message);
            if (maxReconsumeTimes != null) {
                requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
            }
        }

        return requestHeader;
    }
    // messageId : storehost + CommitLogOffset
    public CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            if (handle.getCommitLogOffset() < 0) {
                throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "commit log offset is empty");
            }

            ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
            consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
            consumerSendMsgBackRequestHeader.setGroup(groupName);
            consumerSendMsgBackRequestHeader.setDelayLevel(-1);
            consumerSendMsgBackRequestHeader.setOriginMsgId(messageId);
            // 如果该消息是重试消息，那么需要还原 topic 为 retry topic （ %RETRY%consumerGroup_topic）
            // broker 端在拉取 retry message 之后会将 retry topic 转换为 normal topic
            consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
            consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(0);
            // 将消息发送到 RetryTopic: %RETRY%consumerGroup 或者 DLQTopic : %DLQ%consumerGroup
            // 一个 consumerGroup 对应一个死信队列 DLQTopic : %DLQ%consumerGroup
            future = this.serviceManager.getMessageService().sendMessageBack(
                ctx,
                handle,
                messageId,// storehost + CommitLogOffset
                consumerSendMsgBackRequestHeader,
                timeoutMillis
            ).whenCompleteAsync((remotingCommand, t) -> {
                if (t == null && remotingCommand.getCode() == ResponseCode.SUCCESS) {
                    // 消息已经转移到了 RetryTopic 或者 DLQTopic
                    // 那么原来 topic 的消息就需要 ack
                    this.messagingProcessor.ackMessage(ctx, handle, messageId,
                        groupName, topicName, timeoutMillis);
                }
            }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    private boolean isNeedCheckTopicMessageType(Message message) {
        // enableTopicMessageTypeCheck = true
        return ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()
            && !message.hasProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
    }
}

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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.BatchAckResult;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;

public class AckMessageActivity extends AbstractMessingActivity {

    public AckMessageActivity(MessagingProcessor messagingProcessor, GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            String group = request.getGroup().getName();
            String topic = request.getTopic().getName();
            // enableBatchAck = false
            if (ConfigurationManager.getProxyConfig().isEnableBatchAck()) {
                future = ackMessageInBatch(ctx, group, topic, request);
            } else {
                future = ackMessageOneByOne(ctx, group, topic, request);
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected CompletableFuture<AckMessageResponse> ackMessageInBatch(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        List<ReceiptHandleMessage> handleMessageList = new ArrayList<>(request.getEntriesCount());

        for (AckMessageEntry ackMessageEntry : request.getEntriesList()) {
            String handleString = getHandleString(ctx, group, request, ackMessageEntry);
            handleMessageList.add(new ReceiptHandleMessage(ReceiptHandle.decode(handleString), ackMessageEntry.getMessageId()));
        }
        return this.messagingProcessor.batchAckMessage(ctx, handleMessageList, group, topic)
            .thenApply(batchAckResultList -> {
                AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder();
                Set<Code> responseCodes = new HashSet<>();
                for (BatchAckResult batchAckResult : batchAckResultList) {
                    AckMessageResultEntry entry = convertToAckMessageResultEntry(batchAckResult);
                    responseBuilder.addEntries(entry);
                    responseCodes.add(entry.getStatus().getCode());
                }
                setAckResponseStatus(responseBuilder, responseCodes);
                return responseBuilder.build();
            });
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(BatchAckResult batchAckResult) {
        ReceiptHandleMessage handleMessage = batchAckResult.getReceiptHandleMessage();
        AckMessageResultEntry.Builder resultBuilder = AckMessageResultEntry.newBuilder()
            .setMessageId(handleMessage.getMessageId())
            .setReceiptHandle(handleMessage.getReceiptHandle().getReceiptHandle());
        if (batchAckResult.getProxyException() != null) {
            resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(batchAckResult.getProxyException()));
        } else {
            AckResult ackResult = batchAckResult.getAckResult();
            if (AckStatus.OK.equals(ackResult.getStatus())) {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
            } else {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"));
            }
        }
        return resultBuilder.build();
    }

    protected CompletableFuture<AckMessageResponse> ackMessageOneByOne(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> resultFuture = new CompletableFuture<>();
        // AckMessageResultEntry 表示 ack 的具体 message
        CompletableFuture<AckMessageResultEntry>[] futures = new CompletableFuture[request.getEntriesCount()];
        for (int i = 0; i < request.getEntriesCount(); i++) {
            // 一个消息一个消息的 ack
            // 向 reviveTopic 发送 ack 消息， tag 为 ACK_TAG， ack 之后的消息就不会复活了
            // 但是这里 ack 消息并不会推进原来 topic 对应 queue 的 commitOffset
            // pop 消息的 offset 由 broker 的 popMessageProcessor 负责推进，拉一批往前推一批
            // 由于是消费者组并发 pop,所以需要保证其他消费者可以 pop 的接下来的消息，offset 只能一直向前推
            // 而 ack pop message 只能保证它不会被重试
            futures[i] = processAckMessage(ctx, group, topic, request, request.getEntries(i));
        }
        // 用于等待一组 CompletableFuture 全部正常完成。它非常适合“并行执行多个独立异步任务，
        // 待所有任务结束后再做后续处理”的场景（如批量查询、数据聚合等）。
        // 返回值：一个新的 CompletableFuture<Void>，当所有给定的 CompletableFuture 都正常完成后，
        // 这个返回的 Future 也会正常完成（值为 null）；如果其中任意一个异常完成或取消，则返回的 Future 会以相同的异常完成。
        // 结果类型为 Void：allOf 本身不直接聚合各个 Future 的结果。你需要自己持有原始的 CompletableFuture 引用，
        // 在它们全部完成后手动调用 join() 或 get() 获取每个结果。
        // 异常传播：只要有一个 CompletableFuture 异常完成（或取消），返回的 Future 就会立即以相同异常完成（不会等待其他未完成的 Future）
        // allOf 只负责等待，不负责提取结果。开发者需要自己保存原始 Future 并调用 join() 获取值。
        // CompletableFuture.allOf 是实现并行任务同步点的核心工具。它不直接提供聚合结果，而是提供一个“全部完成”的信号，
        // 你需要自行保存原始任务并提取结果。与 anyOf（竞速）、thenCombine（固定两个任务的合并）相比，allOf 更适合数量不固定、
        // 需要整体等待的场景。结合流式 API，可以优雅地处理批量异步任务。注意异常传播机制：一错俱错。
        CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
            // 所有 ack message 完成之后回调
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
                return;
            }

            Set<Code> responseCodes = new HashSet<>();
            List<AckMessageResultEntry> entryList = new ArrayList<>();
            for (CompletableFuture<AckMessageResultEntry> entryFuture : futures) { // 自己持有原始的 CompletableFuture 引用
                // join()：不支持超时，只能无限阻塞直到任务完成
                // join()：不会响应中断，即使当前线程被中断，它依然会继续等待。这是 join() 的一个缺点，在某些需要及时中断的场景下不适用
                // 使用 join()：
                // 在 流式编程、Lambda 表达式 或 不希望编写烦人的 try-catch 时更简洁。通常与 thenApply、thenCompose 等组合使用，因为异常会被自动包装为 CompletionException，符合函数式风格。
                // 使用 get()：
                // 当你需要处理受检异常（如超时、中断）或在框架代码中必须显式声明异常时。例如在 Runnable 或 Callable 中，或需要严格区分超时和任务失败的不同场景。
                AckMessageResultEntry entryResult = entryFuture.join();
                responseCodes.add(entryResult.getStatus().getCode());
                entryList.add(entryResult);
            }
            AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder()
                .addAllEntries(entryList);
            setAckResponseStatus(responseBuilder, responseCodes);
            resultFuture.complete(responseBuilder.build());
        });
        return resultFuture;
    }

    protected CompletableFuture<AckMessageResultEntry> processAckMessage(ProxyContext ctx, String group, String topic, AckMessageRequest request,
        AckMessageEntry ackMessageEntry) {
        CompletableFuture<AckMessageResultEntry> future = new CompletableFuture<>();

        try {
            // startOffset popTime invisibleTime reviveQid 1( 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2) brokerName queueId msgQueueOffset CommitLogOffset
            String handleString = this.getHandleString(ctx, group, request, ackMessageEntry);
            CompletableFuture<AckResult> ackResultFuture = this.messagingProcessor.ackMessage(
                ctx,
                ReceiptHandle.decode(handleString),
                ackMessageEntry.getMessageId(),
                group,
                topic
            );
            ackResultFuture.thenAccept(result -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, result));
            }).exceptionally(t -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
                return null;
            });
        } catch (Throwable t) {
            future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
        }
        return future;
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry, Throwable throwable) {
        return AckMessageResultEntry.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(throwable))
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .build();
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry,
        AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResultEntry.newBuilder()
                .setMessageId(ackMessageEntry.getMessageId())
                .setReceiptHandle(ackMessageEntry.getReceiptHandle())
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResultEntry.newBuilder()
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"))
            .build();
    }

    protected void setAckResponseStatus(AckMessageResponse.Builder responseBuilder, Set<Code> responseCodes) {
        if (responseCodes.size() > 1) {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.MULTIPLE_RESULTS, Code.MULTIPLE_RESULTS.name()));
        } else if (responseCodes.size() == 1) {
            Code code = responseCodes.stream().findAny().get();
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(code, code.name()));
        } else {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack message result is empty"));
        }
    }

    protected String getHandleString(ProxyContext ctx, String group, AckMessageRequest request, AckMessageEntry ackMessageEntry) {
        String handleString = ackMessageEntry.getReceiptHandle();
        GrpcClientChannel channel = grpcChannelManager.getChannel(ctx.getClientID());
        if (channel != null) {
            // 当消息 ack 之后，将 msgID -> messageReceiptHandle 从 ReceiptHandleGroup 中删除
            MessageReceiptHandle messageReceiptHandle = messagingProcessor.removeReceiptHandle(ctx, channel, group, ackMessageEntry.getMessageId(), ackMessageEntry.getReceiptHandle());
            if (messageReceiptHandle != null) {
                // startOffset popTime invisibleTime reviveQid 1( 0 表示 NORMAL_TOPIC，1 表示 RETRY_TOPIC，2 表示 RETRY_TOPIC_V2) brokerName queueId msgQueueOffset CommitLogOffset
                handleString = messageReceiptHandle.getReceiptHandleStr();
            }
        }
        return handleString;
    }
}

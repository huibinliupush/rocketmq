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

package org.apache.rocketmq.broker.longpolling;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import static org.apache.rocketmq.broker.longpolling.PollingResult.NOT_POLLING;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_FULL;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_SUC;
import static org.apache.rocketmq.broker.longpolling.PollingResult.POLLING_TIMEOUT;

public class PopLongPollingService extends ServiceThread {

    private static final Logger POP_LOGGER =
        LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    // PopMessageProcessor
    private final NettyRequestProcessor processor;
    // topic -> cid（consumerGroup） -> Byte.MIN_VALUE
    // queueId = -1 表示读取所有消费队列
    // 主要用来分辨 topic 下有多少 consumerGroup ，pop模式下消费者可以消费所有队列，所以这里的 queueId 都是 -1
    // polling 的时候填充
    private final ConcurrentLinkedHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap;
    // key : topic@cid（consumerGroup）@queueId ， value 不能超过 1024 popPollingSize
    // pop 模式下 queueId = -1 ，表示consumerGroup下的消费者可以消费所有队列
    // 所以对应的 topic@consumeGroup@queueId 中存放的是同一consumeGroup下所有消费者的 PopRequest(consumeGroup下所有消费者均可以消费该队列)
    // value 跳表中的 PopRequest 排序规则：
    // 1. 过期时间越近的排在最前面
    // 2. op(全局计数COUNTER) 越小排在最前面
    private final ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    // 上一次执行 cleanUnusedResource 方法的时间戳
    private long lastCleanTime = 0;
    // 当前所有队列中正在 polling 的 popRequest(还未处理)
    // popRequest timeout 了会由 run 方法执行，随后 totalPollingNum - 1
    // 不能超过 maxPopPollingSize = 100000
    private final AtomicLong totalPollingNum = new AtomicLong(0);
    // 从 pollingMap 中 poll 第一个还是最后一个 PopRequest
    private final boolean notifyLast;

    public PopLongPollingService(BrokerController brokerController, NettyRequestProcessor processor, boolean notifyLast) {
        this.brokerController = brokerController;
        // PopMessageProcessor
        this.processor = processor;
        // 100000 topic default,  100000 lru topic + cid（consumerGroup） + qid(queueId)
        // popPollingMapSize = 100000
        this.topicCidMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentHashMap<String, Byte>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize() * 2L).build();
        this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>()
            .maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        // false
        this.notifyLast = notifyLast;
    }

    @Override
    public String getServiceName() {
        if (brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopLongPollingService.class.getSimpleName();
        }
        return PopLongPollingService.class.getSimpleName();
    }

    /**
     * run 方法被动处理过期的 popRequest
     * 首先消费者组的消费者都可以消费所有队列，每个队列中的消息都会均匀的平摊给每个消费者组中的所有消费者
     * 所有消费者组中的所有消费者发起 popRequest, 这里会按照 topic@consumeGroup@queueId 的维度将对应的 popRequest 组织在 pollingMap 中
     * 这里的 run 方法会每隔 20ms 去拉取 pollingMap 中的每个 ConcurrentSkipListSet（topic@cid@queueId 维度）
     * 将其中的  timeout popRequest 重新触发 PopMessageProcessor.processRequest
     * 这样一来，同一个 cid(consumerGroup) 也就是同一个消费者就可以消费所有队列了
     *
     * pop 模式下 queueId = -1 ，表示consumerGroup下的消费者可以消费所有队列
     * 所以对应的 topic@consumeGroup@queueId 中存放的是同一consumeGroup下所有消费者的 PopRequest(consumeGroup下所有消费者均可以消费该队列)
     * 执行每个 consumeGroup下过期的 PopRequest ，只要遇到没有过期的就跳出循环，继续处理下一个 consumeGroup 下的所有 PopRequest
     *
     * 而 pullRequestHoldService 还是需要消费者绑定队列，所以它里面的组织结构是 topic@queueId 中存放的是不同 consumequeue 下所有
     * 绑定该 queue 的消费者 PullRequest （只有绑定到该队列的消费者才可以消费，但不区分consumeGroup）
     *
     * notifyMessageArriving --> pop 消费逻辑的精髓就在这里
     *
     * 每次消息到来，只会通知 consumeGroup 下的一个消费者 PopRequest(不是所有)
     * 第二个消息到来，继续通知下一个消费者 PopRequest
     * 这样 consumeGroup 下的所有消费者就能均匀的平摊所有队列中的消息了
     *
     * 不同的 consumeGroup 下的消费者拉取的位点不一样
     * 所以这里需要判断拉取位点
     * pull模式下，消费者是需要绑定queue的
     * pop模式就不需要判断位点，因为它只区分consume Group
     * 不会区分queue,所有消费者都可以消费 queue
     * queue有消息就拉（只会通知每个consumeGroup下的一个popRequest）
     *
     * org.apache.rocketmq.broker.longpolling.PopLongPollingService#notifyMessageArriving(java.lang.String, int, java.lang.String, boolean, java.lang.Long, long, byte[], java.util.Map, org.apache.rocketmq.remoting.CommandCallback)
     * */
    @Override
    public void run() {
        int i = 0;
        while (!this.stopped) {
            try {
                this.waitForRunning(20);
                i++;
                if (pollingMap.isEmpty()) {
                    continue;
                }
                // 重新统计 TotalPollingNum
                long tmpTotalPollingNum = 0;
                // 挨个获取所有consumeGroup中的 PopRequest
                // key : topic@cid@queueId
                for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                    String key = entry.getKey();
                    // 某个 consumeGroup 下的所有 PopRequest(来自不同的消费者)
                    ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                    if (popQ == null) {
                        continue;
                    }
                    PopRequest first;
                    // 执行过期的 PopRequest ，只要遇到没有过期的就跳出循环，继续处理下一个 consumeGroup 下的所有 PopRequest
                    do {
                        first = popQ.pollFirst();
                        if (first == null) {
                            break;
                        }
                        if (!first.isTimeout()) {
                            // PopRequest 没有 time out 则不执行，跳出执行 do while 循环,继续处理下一个 queue 上的 pop request
                            if (popQ.add(first)) {
                                // PopRequest 在 ConcurrentSkipListSet 中是按照过期时间排好序的
                                // 只要第一个没过期，后面的就不用看了
                                break;
                            } else {
                                POP_LOGGER.info("polling, add fail again: {}", first);
                            }
                        }
                        if (brokerController.getBrokerConfig().isEnablePopLog()) {
                            POP_LOGGER.info("timeout , wakeUp polling : {}", first);
                        }
                        // PopRequest time out 了才会去执行
                        totalPollingNum.decrementAndGet();
                        // 重新触发 PopMessageProcessor.processRequest
                        wakeUp(first);
                    }
                    while (true);
                    // 到这里该 queue 对应的 PopRequest 到期的就都执行了
                    // 现在 popQ 中存留的 PopRequest 就是还未 timeout 正在 polling 的请求
                    if (i >= 100) {
                        long tmpPollingNum = popQ.size();
                        // 更新 TotalPollingNum
                        tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
                        if (tmpPollingNum > 100) {
                            POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
                        }
                    }
                }
                // 到这里所有 queue 对应的 popRequst 该处理的就全处理完了
                if (i >= 100) {
                    POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}",
                        pollingMap.size(), tmpTotalPollingNum, totalPollingNum.get(),
                        Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
                    // 更新 TotalPollingNum
                    totalPollingNum.set(tmpTotalPollingNum);
                    i = 0;
                }

                // clean unused 每隔 5 分钟
                if (lastCleanTime == 0 || System.currentTimeMillis() - lastCleanTime > 5 * 60 * 1000) {
                    // 从 topicCidMap 中清除不存在 topic 以及不存在的 consumeGroup
                    cleanUnusedResource();
                }
            } catch (Throwable e) {
                POP_LOGGER.error("checkPolling error", e);
            }
        }
        // stopped = true clean all;
        try {
            for (Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry : pollingMap.entrySet()) {
                ConcurrentSkipListSet<PopRequest> popQ = entry.getValue();
                PopRequest first;
                while ((first = popQ.pollFirst()) != null) {
                    wakeUp(first);
                }
            }
        } catch (Throwable e) {
        }
    }

    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId) {
        this.notifyMessageArrivingWithRetryTopic(topic, queueId, -1L, null, 0L, null, null);
    }
    // offset 为消息在对应 queueId 中的个数（并不是 bytes 单位），而是 queue 中的第几个消息
    public void notifyMessageArrivingWithRetryTopic(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String notifyTopic;
        if (KeyBuilder.isPopRetryTopicV2(topic)) {
            // 从 RetryTopic 中提取 NormalTopic
            notifyTopic = KeyBuilder.parseNormalTopic(topic);
        } else {
            notifyTopic = topic;
        }
        notifyMessageArriving(notifyTopic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
    // offset 为消息在对应 queueId 中的个数（并不是 bytes 单位），而是 queue 中的第几个消息
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // 所有 consumeGroup -> queueId
        // 获取 topic 下所有的 consumeGroup
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(topic);
        if (cids == null) {
            return;
        }
        // 800
        long interval = brokerController.getBrokerConfig().getPopLongPollingForceNotifyInterval();
        // 每 800 个消息 force 一下
        boolean force = interval > 0L && offset % interval == 0L;
        // 遍历所有 consumeGroup
        for (Map.Entry<String, Byte> cid : cids.entrySet()) {
            if (queueId >= 0) {
                // queueId = -1 表示读取所有消费队列
                notifyMessageArriving(topic, -1, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
            }
            notifyMessageArriving(topic, queueId, cid.getKey(), force, tagsCode, msgStoreTime, filterBitMap, properties);
        }
    }

    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, false, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }

    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        return notifyMessageArriving(topic, queueId, cid, force, tagsCode, msgStoreTime, filterBitMap, properties, null);
    }
    // 不同的 consumeGroup 下的消费者拉取的位点不一样
    // 所以这里需要判断拉取位点
    // pull模式下，消费者是需要绑定queue的
    // pop模式就不需要判断位点，因为它只区分consume Group
    // 不会区分queue,所有消费者都可以消费 queue
    // queue有消息就拉（只会通知每个consumeGroup下的一个popRequest）
    public boolean notifyMessageArriving(final String topic, final int queueId, final String cid, boolean force,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties, CommandCallback callback) {
        // topic@cid@queueId
        // queueId = -1 表示读取所有消费队列
        ConcurrentSkipListSet<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid, queueId));
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return false;
        }
        // 取出第一个 PopRequest，前提是 popRequest.getChannel().isActive
        // 消息到来，只会通知 consumeGroup 下的一个消费者 PopRequest(不是所有)
        // 第二个消息到来，继续通知下一个消费者 PopRequest
        // 这样 consumeGroup 下的所有消费者就能均匀的平摊所有队列中的消息了
        PopRequest popRequest = pollRemotingCommands(remotingCommands);
        if (popRequest == null) {
            return false;
        }
        // 每 800 个消息 force 一下
        if (!force && popRequest.getMessageFilter() != null && popRequest.getSubscriptionData() != null) {
            boolean match = popRequest.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
            if (match && properties != null) {
                match = popRequest.getMessageFilter().isMatchedByCommitLog(null, properties);
            }
            if (!match) {
                // 通知的消息与 pop 请求不匹配，则重新添加回去，继续 long polling
                remotingCommands.add(popRequest);
                totalPollingNum.incrementAndGet();
                return false;
            }
        }
        // false
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("lock release, new msg arrive, wakeUp: {}", popRequest);
        }
        // 重新触发 PopMessageProcessor.processRequest
        // 消息到来，只会通知 consumeGroup 下的一个消费者 PopRequest(不是所有)
        // 第二个消息到来，继续通知下一个消费者 PopRequest
        // 这样 consumeGroup 下的所有消费者就能均匀的平摊所有队列中的消息了
        return wakeUp(popRequest, callback);
    }

    public boolean wakeUp(final PopRequest request) {
        return wakeUp(request, null);
    }

    public boolean wakeUp(final PopRequest request, CommandCallback callback) {
        if (request == null || !request.complete()) {
            return false;
        }

        if (callback != null && request.getRemotingCommand() != null) {
            if (request.getRemotingCommand().getCallbackList() == null) {
                request.getRemotingCommand().setCallbackList(new ArrayList<>());
            }
            request.getRemotingCommand().getCallbackList().add(callback);
        }

        if (!request.getCtx().channel().isActive()) {
            return false;
        }
        // 异步重新触发 pop 逻辑，防止阻塞 notify 线程
        Runnable run = () -> {
            try {
                final RemotingCommand response = processor.processRequest(request.getCtx(), request.getRemotingCommand());
                if (response != null) {
                    response.setOpaque(request.getRemotingCommand().getOpaque());
                    response.markResponseType();
                    NettyRemotingAbstract.writeResponse(request.getChannel(), request.getRemotingCommand(), response, future -> {
                        if (!future.isSuccess()) {
                            POP_LOGGER.error("ProcessRequestWrapper response to {} failed", request.getChannel().remoteAddress(), future.cause());
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    });
                }
            } catch (Exception e1) {
                POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
            }
        };
        // 16 + PROCESSOR_NUMBER * 2 , 10万队列
        this.brokerController.getPullMessageExecutor().submit(
            new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
        return true;
    }

    /**
     * @param ctx
     * @param remotingCommand
     * @param requestHeader
     * @return
     */
    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader) {
        return this.polling(ctx, remotingCommand, requestHeader, null, null);
    }

    public PollingResult polling(final ChannelHandlerContext ctx, RemotingCommand remotingCommand,
        final PollingHeader requestHeader, SubscriptionData subscriptionData, MessageFilter messageFilter) {
        if (requestHeader.getPollTime() <= 0 || this.isStopped()) {
            return NOT_POLLING;
        }
        ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic());
        if (cids == null) {
            cids = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Byte> old = topicCidMap.putIfAbsent(requestHeader.getTopic(), cids);
            if (old != null) {
                cids = old;
            }
        }
        cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
        // BornTime + PollTime （时间戳）
        long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
        final PopRequest request = new PopRequest(remotingCommand, ctx, expired, subscriptionData, messageFilter);
        // maxPopPollingSize = 100000
        // broker 总共可以承载的最大 polling request 个数
        boolean isFull = totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize();
        if (isFull) {
            POP_LOGGER.info("polling {}, result POLLING_FULL, total:{}", remotingCommand, totalPollingNum.get());
            // totalPollingNum >= 100000
            return POLLING_FULL;
        }
        boolean isTimeout = request.isTimeout();
        if (isTimeout) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
            }
            return POLLING_TIMEOUT;
        }
        // Topic@ConsumerGroup@QueueId
        String key = KeyBuilder.buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
            requestHeader.getQueueId());
        // 1. 过期时间越近的排在最前面
        // 2. op(全局计数COUNTER) 越小排在最前面
        ConcurrentSkipListSet<PopRequest> queue = pollingMap.get(key);
        if (queue == null) {
            queue = new ConcurrentSkipListSet<>(PopRequest.COMPARATOR);
            ConcurrentSkipListSet<PopRequest> old = pollingMap.putIfAbsent(key, queue);
            if (old != null) {
                queue = old;
            }
        } else {
            // check size
            int size = queue.size();
            // 不能超过 1024
            // 单个 queue 的 polling request 个数不能超过 1024
            if (size > brokerController.getBrokerConfig().getPopPollingSize()) {
                POP_LOGGER.info("polling {}, result POLLING_FULL, singleSize:{}", remotingCommand, size);
                return POLLING_FULL;
            }
        }
        if (queue.add(request)) {
            remotingCommand.setSuspended(true);
            totalPollingNum.incrementAndGet();
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
            }
            return POLLING_SUC;
        } else {
            POP_LOGGER.info("polling {}, result POLLING_FULL, add fail, {}", request, queue);
            return POLLING_FULL;
        }
    }

    public ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> getPollingMap() {
        return pollingMap;
    }
    // 从 topicCidMap 中清除不存在 topic 以及不存在的 consumeGroup
    private void cleanUnusedResource() {
        try {
            {
                Iterator<Map.Entry<String, ConcurrentHashMap<String, Byte>>> topicCidMapIter = topicCidMap.entrySet().iterator();
                while (topicCidMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentHashMap<String, Byte>> entry = topicCidMapIter.next();
                    String topic = entry.getKey();
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in topicCidMap!", topic);
                        topicCidMapIter.remove();
                        continue;
                    }
                    Iterator<Map.Entry<String, Byte>> cidMapIter = entry.getValue().entrySet().iterator();
                    while (cidMapIter.hasNext()) {
                        Map.Entry<String, Byte> cidEntry = cidMapIter.next();
                        String cid = cidEntry.getKey();
                        if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                            POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in topicCidMap!", cid, topic);
                            cidMapIter.remove();
                        }
                    }
                }
            }

            {
                Iterator<Map.Entry<String, ConcurrentSkipListSet<PopRequest>>> pollingMapIter = pollingMap.entrySet().iterator();
                while (pollingMapIter.hasNext()) {
                    Map.Entry<String, ConcurrentSkipListSet<PopRequest>> entry = pollingMapIter.next();
                    if (entry.getKey() == null) {
                        continue;
                    }
                    String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
                    if (keyArray.length != 3) {
                        continue;
                    }
                    String topic = keyArray[0];
                    String cid = keyArray[1];
                    if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        POP_LOGGER.info("remove nonexistent topic {} in pollingMap!", topic);
                        pollingMapIter.remove();
                        continue;
                    }
                    if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                        POP_LOGGER.info("remove nonexistent subscription group {} of topic {} in pollingMap!", cid, topic);
                        pollingMapIter.remove();
                    }
                }
            }
        } catch (Throwable e) {
            POP_LOGGER.error("cleanUnusedResource", e);
        }

        lastCleanTime = System.currentTimeMillis();
    }

    private PopRequest pollRemotingCommands(ConcurrentSkipListSet<PopRequest> remotingCommands) {
        if (remotingCommands == null || remotingCommands.isEmpty()) {
            return null;
        }

        PopRequest popRequest;
        do {
            if (notifyLast) {
                popRequest = remotingCommands.pollLast();
            } else {
                // 默认
                popRequest = remotingCommands.pollFirst();
            }
            totalPollingNum.decrementAndGet();
        } while (popRequest != null && !popRequest.getChannel().isActive());

        return popRequest;
    }
}

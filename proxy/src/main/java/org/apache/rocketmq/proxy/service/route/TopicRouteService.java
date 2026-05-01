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
package org.apache.rocketmq.proxy.service.route;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.Resolver;
import org.apache.rocketmq.client.latency.ServiceDetector;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class TopicRouteService extends AbstractStartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final MQClientAPIFactory mqClientAPIFactory;
    private MQFaultStrategy mqFaultStrategy;

    protected final LoadingCache<String /* topicName */, MessageQueueView> topicCache;
    protected final ScheduledExecutorService scheduledExecutorService;
    // PROCESSOR_NUMBER
    protected final ThreadPoolExecutor cacheRefreshExecutor;

    public TopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        this.scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("TopicRouteService_")
        );
        // PROCESSOR_NUMBER
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicRouteServiceThreadPoolNums(),// PROCESSOR_NUMBER
            config.getTopicRouteServiceThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicRouteCacheRefresh",
            config.getTopicRouteServiceThreadPoolQueueCapacity()// 5000
        );
        this.mqClientAPIFactory = mqClientAPIFactory;
        /**
         * maximumSize: 当条目数超过限制时，会依据W-TinyLFU算法进行驱逐。
         *
         * refreshAfterWrite 是 Caffeine 缓存提供的一种 异步刷新 策略。它允许缓存的条目在写入后经过指定时间，变得 陈旧但尚未被移除，下次访问时会触发后台异步刷新，而访问者 立即获得旧值（不会等待刷新完成）。
         *
         * 这与 expireAfterWrite 有着本质区别：后者在过期后，下一次访问会 阻塞等待 重新加载新值（或返回 null/抛出异常）；而 refreshAfterWrite 则 永远不阻塞读请求，始终快速返回（旧值或刚刷新的新值）。
         *
         * refreshAfterWrite：
         * 1. 入后 T1 时间内: 直接返回缓存值
         * 2. 超过 T1 后第一次访问: 立即返回现有（可能过时的）值，同时触发后台异步加载新值
         * 3. 后续访问: 在异步加载完成前，继续返回旧值；加载完成后，返回新值
         *
         * 仅当访问时触发：refreshAfterWrite 不会主动定时刷新，只有在 get 操作检测到上次写入时间已超过设定的刷新间隔时才会触发。
         *
         * build()（同步加载）: LoadingCache<String, User> cache = Caffeine.newBuilder...build ; User user = cache.get("alice")
         * buildAsync()（异步加载）: AsyncLoadingCache<String, User> cache = Caffeine.newBuilder...buildAsync ; CompletableFuture<User> future = cache.get("alice");
         *
         * CacheLoader 是 Caffeine 中定义 如何从外部源加载数据 的核心接口。它与 LoadingCache 或 AsyncLoadingCache 配合，实现“当缓存中没有某个 key 时，自动调用 load 方法获取值并存入缓存”的语义。
         *
         * 简单说：CacheLoader 定义了缓存缺失时（或需要刷新时）的数据获取逻辑。
         *
         * 异步加载：AsyncCacheLoader,对于 I/O 密集型场景（网络、数据库），可以使用 AsyncCacheLoader，它返回 CompletableFuture<V>，避免阻塞调用线程。
         *
         * AsyncCacheLoader<String, User> loader = (key, executor) ->
         *     CompletableFuture.supplyAsync(() -> userDao.findById(key), executor);
         *
         * AsyncLoadingCache<String, User> cache = Caffeine.newBuilder()
         *     .buildAsync(loader);
         *
         * refreshAfterWrite 也完全支持 AsyncCacheLoader，刷新会在后台线程池执行
         * */
        this.topicCache = Caffeine.newBuilder().maximumSize(config.getTopicRouteServiceCacheMaxNum()) // 20000
            .expireAfterAccess(config.getTopicRouteServiceCacheExpiredSeconds(), TimeUnit.SECONDS)// 300 ,最后一次访问后固定时长过期
            .refreshAfterWrite(config.getTopicRouteServiceCacheRefreshSeconds(), TimeUnit.SECONDS)// 20
            .executor(cacheRefreshExecutor)
            .build(new CacheLoader<String, MessageQueueView>() { // 同步加载
                // 同步加载单个 key
                @Override
                public @Nullable MessageQueueView load(String topic) throws Exception {
                    try {
                        // 从 name server 中获取 topic 下的 routedata
                        TopicRouteData topicRouteData = mqClientAPIFactory.getClient().getTopicRouteInfoFromNameServer(topic, Duration.ofSeconds(3).toMillis());
                        return buildMessageQueueView(topic, topicRouteData);
                    } catch (Exception e) {
                        if (TopicRouteHelper.isTopicNotExistError(e)) {
                            return MessageQueueView.WRAPPED_EMPTY_QUEUE;
                        }
                        throw e;
                    }
                }
                // 同步刷新方法（极少直接覆盖）
                @Override
                public @Nullable MessageQueueView reload(@NonNull String key,
                    @NonNull MessageQueueView oldValue) throws Exception {
                    try {
                        return load(key);
                    } catch (Exception e) {
                        log.warn(String.format("reload topic route from namesrv. topic: %s", key), e);
                        // 刷新失败，保留旧值
                        return oldValue;
                    }
                }
            });
        // Detect whether the remote service state is normal
        ServiceDetector serviceDetector = new ServiceDetector() {
            // 检测对应 broker 是否拥有对应 topic
            @Override
            public boolean detect(String endpoint, long timeoutMillis) {
                // pickup one topic in the topic cache
                Optional<String> candidateTopic = pickTopic();
                if (!candidateTopic.isPresent()) {
                    return false;
                }
                try {
                    GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
                    requestHeader.setTopic(candidateTopic.get());
                    requestHeader.setQueueId(0);
                    // 获取 broker （endpoint）下 topic 下对应 queueid 的 maxOffset(index)
                    Long maxOffset = mqClientAPIFactory.getClient().getMaxOffset(endpoint, requestHeader, timeoutMillis).get();
                    return true;
                } catch (Exception e) {
                    // broker 没有该 topic
                    return false;
                }
            }
        };
        mqFaultStrategy = new MQFaultStrategy(extractClientConfigFromProxyConfig(config), new Resolver() {
            @Override
            public String resolve(String name) {
                try {
                    // name （副本集）对应的 master address
                    String brokerAddr = getBrokerAddr(ProxyContext.createForInner("MQFaultStrategy"), name);
                    return brokerAddr;
                } catch (Exception e) {
                    return null;
                }
            }
        }, serviceDetector);
        this.init();
    }

    // pickup one topic in the topic cache
    private Optional<String> pickTopic() {
        if (topicCache.asMap().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(topicCache.asMap().keySet().iterator().next());
    }

    protected void init() {
        this.appendShutdown(this.scheduledExecutorService::shutdown);
        this.appendStartAndShutdown(this.mqClientAPIFactory);
    }

    @Override
    public void shutdown() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            mqFaultStrategy.shutdown();
        }
    }

    @Override
    public void start() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            this.mqFaultStrategy.startDetector();
        }
    }
    // related to proxy's send strategy in cluster mode.
    public ClientConfig extractClientConfigFromProxyConfig(ProxyConfig proxyConfig) {
        ClientConfig tempClientConfig = new ClientConfig();
        // sendLatencyEnable = false
        tempClientConfig.setSendLatencyEnable(proxyConfig.getSendLatencyEnable());
        // startDetectorEnable = false
        tempClientConfig.setStartDetectorEnable(proxyConfig.getStartDetectorEnable());
        // detectTimeout = 200
        tempClientConfig.setDetectTimeout(proxyConfig.getDetectTimeout());
        // detectInterval = 2 * 1000;
        tempClientConfig.setDetectInterval(proxyConfig.getDetectInterval());
        return tempClientConfig;
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                boolean reachable) {
        checkSendFaultToleranceEnable();
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
    }

    public void checkSendFaultToleranceEnable() {
        boolean hotLatencySwitch = ConfigurationManager.getProxyConfig().isSendLatencyEnable();
        boolean hotDetectorSwitch = ConfigurationManager.getProxyConfig().isStartDetectorEnable();
        this.mqFaultStrategy.setSendLatencyFaultEnable(hotLatencySwitch);
        this.mqFaultStrategy.setStartDetectorEnable(hotDetectorSwitch);
    }

    public MQFaultStrategy getMqFaultStrategy() {
        return this.mqFaultStrategy;
    }

    public MessageQueueView getAllMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        return getCacheMessageQueueWrapper(this.topicCache, topicName);
    }

    public abstract MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) throws Exception;

    public abstract ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception;

    public abstract String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception;

    public abstract AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx, MessageQueue messageQueue) throws Exception;

    protected static MessageQueueView getCacheMessageQueueWrapper(LoadingCache<String, MessageQueueView> topicCache,
        String key) throws Exception {
        MessageQueueView res = topicCache.get(key);
        if (res != null && res.isEmptyCachedQueue()) {
            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST,
                "No topic route info in name server for the topic: " + key);
        }
        return res;
    }

    protected static boolean isTopicRouteValid(TopicRouteData routeData) {
        return routeData != null && routeData.getQueueDatas() != null && !routeData.getQueueDatas().isEmpty()
            && routeData.getBrokerDatas() != null && !routeData.getBrokerDatas().isEmpty();
    }

    protected MessageQueueView buildMessageQueueView(String topic, TopicRouteData topicRouteData) {
        if (isTopicRouteValid(topicRouteData)) {
            MessageQueueView tmp = new MessageQueueView(topic, topicRouteData, TopicRouteService.this.getMqFaultStrategy());
            log.debug("load topic route from namesrv. topic: {}, queue: {}", topic, tmp);
            return tmp;
        }
        return MessageQueueView.WRAPPED_EMPTY_QUEUE;
    }
}

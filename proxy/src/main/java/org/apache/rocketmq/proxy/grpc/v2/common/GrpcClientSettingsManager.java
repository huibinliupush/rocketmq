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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.Metric;
import apache.rocketmq.v2.Settings;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.MetricCollectorMode;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class GrpcClientSettingsManager extends ServiceThread implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    // 由 org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication.telemetry 填充 ClientSetting
    // key : clientId
    // 首先用 proxy 端默认的 SubscriptionGroupConfig 与客户端传递过来的 setting 进行合并，缓存在 CLIENT_SETTINGS_MAP 中
    protected static final Map<String, Settings> CLIENT_SETTINGS_MAP = new ConcurrentHashMap<>();

    private final MessagingProcessor messagingProcessor;

    public GrpcClientSettingsManager(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
    }

    public Settings getRawClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.get(clientId);
    }

    public Settings getClientSettings(ProxyContext ctx) {
        String clientId = ctx.getClientID();
        // 由 org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication.telemetry 填充 ClientSetting
        Settings settings = getRawClientSettings(clientId);
        if (settings == null) {
            return null;
        }
        if (settings.hasPublishing()) {
            settings = mergeProducerData(settings);
        } else if (settings.hasSubscription()) {
            // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
            // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
            // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
            // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
            settings = mergeSubscriptionData(ctx, settings, settings.getSubscription().getGroup().getName());
        }
        return mergeMetric(settings);
    }

    protected static Settings mergeProducerData(Settings settings) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        Settings.Builder builder = settings.toBuilder();

        builder.getBackoffPolicyBuilder()
            .setMaxAttempts(config.getGrpcClientProducerMaxAttempts())
            .setExponentialBackoff(ExponentialBackoff.newBuilder()
                .setInitial(Durations.fromMillis(config.getGrpcClientProducerBackoffInitialMillis()))
                .setMax(Durations.fromMillis(config.getGrpcClientProducerBackoffMaxMillis()))
                .setMultiplier(config.getGrpcClientProducerBackoffMultiplier())
                .build());

        builder.getPublishingBuilder()
            .setValidateMessageType(config.isEnableTopicMessageTypeCheck())
            .setMaxBodySize(config.getMaxMessageSize());
        return builder.build();
    }
    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
    protected Settings mergeSubscriptionData(ProxyContext ctx, Settings settings, String consumerGroup) {
        // 随机获取集群（brokerClusterName）中的一个副本集，获取副本集中 master broker
        // 向 master broker 获取 consumerGroup 的订阅关系配置（由 admin 命令创建填充）
        // broker 启动的时候会在创建 topicConfigManger 的时候初始化系统级 topic 包括 brokerClusterName
        // 这样 proxy 可以通过 brokerClusterName topic 获取到整个集群所有副本集拓扑
        SubscriptionGroupConfig config = this.messagingProcessor.getSubscriptionGroupConfig(ctx, consumerGroup);
        if (config == null) {
            return settings;
        }
        // broker 端存储的 consumerGroup 订阅关系与本地配置合并
        return mergeSubscriptionData(settings, config);
    }

    protected Settings mergeMetric(Settings settings) {
        // Construct metric according to the proxy config
        final ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        final MetricCollectorMode metricCollectorMode =
            MetricCollectorMode.getEnumByString(proxyConfig.getMetricCollectorMode());
        final String metricCollectorAddress = proxyConfig.getMetricCollectorAddress();
        final Metric.Builder metricBuilder = Metric.newBuilder();
        switch (metricCollectorMode) {
            case ON:
                final String[] split = metricCollectorAddress.split(":");
                final String host = split[0];
                final int port = Integer.parseInt(split[1]);
                Address address = Address.newBuilder().setHost(host).setPort(port).build();
                final Endpoints endpoints = Endpoints.newBuilder().setScheme(AddressScheme.IPv4)
                    .addAddresses(address).build();
                metricBuilder.setOn(true).setEndpoints(endpoints);
                break;
            case PROXY:
                metricBuilder.setOn(true).setEndpoints(settings.getAccessPoint());
                break;
            case OFF:
            default:
                metricBuilder.setOn(false);
                break;
        }
        Metric metric = metricBuilder.build();
        return settings.toBuilder().setMetric(metric).build();
    }
    // Settings 要返回给客户端的相关配置类
    // SubscriptionGroupConfig 远程 broker 端的配置（由 admin 创建 consumerGroup 的时候在 broker 填充）
    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
    protected static Settings mergeSubscriptionData(Settings settings, SubscriptionGroupConfig groupConfig) {
        Settings.Builder resultSettingsBuilder = settings.toBuilder();
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        resultSettingsBuilder.getSubscriptionBuilder()
            .setReceiveBatchSize(config.getGrpcClientConsumerLongPollingBatchSize()) // 32
            .setLongPollingTimeout(Durations.fromMillis(config.getGrpcClientConsumerMaxLongPollingTimeoutMillis())) // 20s
            .setFifo(groupConfig.isConsumeMessageOrderly());// 默认 false

        resultSettingsBuilder.getBackoffPolicyBuilder().setMaxAttempts(groupConfig.getRetryMaxTimes() + 1); // 16+1

        GroupRetryPolicy groupRetryPolicy = groupConfig.getGroupRetryPolicy();
        if (groupRetryPolicy.getType().equals(GroupRetryPolicyType.EXPONENTIAL)) {
            ExponentialRetryPolicy exponentialRetryPolicy = groupRetryPolicy.getExponentialRetryPolicy();
            if (exponentialRetryPolicy == null) {
                exponentialRetryPolicy = new ExponentialRetryPolicy();
            }
            resultSettingsBuilder.getBackoffPolicyBuilder().setExponentialBackoff(convertToExponentialBackoff(exponentialRetryPolicy));
        } else {
            CustomizedRetryPolicy customizedRetryPolicy = groupRetryPolicy.getCustomizedRetryPolicy();
            if (customizedRetryPolicy == null) {
                customizedRetryPolicy = new CustomizedRetryPolicy();
            }
            resultSettingsBuilder.getBackoffPolicyBuilder().setCustomizedBackoff(convertToCustomizedRetryPolicy(customizedRetryPolicy));
        }

        return resultSettingsBuilder.build();
    }

    protected static ExponentialBackoff convertToExponentialBackoff(ExponentialRetryPolicy retryPolicy) {
        return ExponentialBackoff.newBuilder()
            .setInitial(Durations.fromMillis(retryPolicy.getInitial()))
            .setMax(Durations.fromMillis(retryPolicy.getMax()))
            .setMultiplier(retryPolicy.getMultiplier())
            .build();
    }

    protected static CustomizedBackoff convertToCustomizedRetryPolicy(CustomizedRetryPolicy retryPolicy) {
        List<Duration> durationList = Arrays.stream(retryPolicy.getNext())
            .mapToObj(Durations::fromMillis).collect(Collectors.toList());
        return CustomizedBackoff.newBuilder()
            .addAllNext(durationList)
            .build();
    }

    public void updateClientSettings(ProxyContext ctx, String clientId, Settings settings) {
        if (settings.hasSubscription()) {
            // createDefaultConsumerSettingsBuilder 获取订阅关系的默认配置
            // SubscriptionGroupConfig 是由 admin 创建消费者组的时候填充到 broker 的
            // 默认 settings 与客户端发送过来的 settings 进行合并
            settings = createDefaultConsumerSettingsBuilder().mergeFrom(settings).build();
        }
        CLIENT_SETTINGS_MAP.put(clientId, settings);
    }

    protected Settings.Builder createDefaultConsumerSettingsBuilder() {
        // 获取订阅关系的默认配置
        // SubscriptionGroupConfig 是由 admin 创建消费者组的时候填充到 broker 的
        // 这里使用 proxy 端默认的 SubscriptionGroupConfig 与客户端的 setting 进行合并
        return mergeSubscriptionData(Settings.newBuilder().getDefaultInstanceForType(), new SubscriptionGroupConfig())
            .toBuilder();
    }

    public Settings removeAndGetRawClientSettings(String clientId) {
        return CLIENT_SETTINGS_MAP.remove(clientId);
    }

    public Settings removeAndGetClientSettings(ProxyContext ctx) {
        String clientId = ctx.getClientID();
        Settings settings = this.removeAndGetRawClientSettings(clientId);
        if (settings == null) {
            return null;
        }
        if (settings.hasSubscription()) {
            settings = mergeSubscriptionData(ctx, settings, settings.getSubscription().getGroup().getName());
        }
        return mergeMetric(settings);
    }

    @Override
    public String getServiceName() {
        return "GrpcClientSettingsManagerCleaner";
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            this.waitForRunning(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Override
    protected void onWaitEnd() {
        Set<String> clientIdSet = CLIENT_SETTINGS_MAP.keySet();
        for (String clientId : clientIdSet) {
            try {
                CLIENT_SETTINGS_MAP.computeIfPresent(clientId, (clientIdKey, settings) -> {
                    if (!settings.getClientType().equals(ClientType.PUSH_CONSUMER) && !settings.getClientType().equals(ClientType.SIMPLE_CONSUMER)) {
                        return settings;
                    }
                    String consumerGroup = settings.getSubscription().getGroup().getName();
                    ConsumerGroupInfo consumerGroupInfo = this.messagingProcessor.getConsumerGroupInfo(
                        ProxyContext.createForInner(this.getClass()),
                        consumerGroup
                    );
                    if (consumerGroupInfo == null || consumerGroupInfo.findChannel(clientId) == null) {
                        log.info("remove unused grpc client settings. group:{}, settings:{}", consumerGroupInfo, settings);
                        return null;
                    }
                    return settings;
                });
            } catch (Throwable t) {
                log.error("check expired grpc client settings failed. clientId:{}", clientId, t);
            }
        }
    }
}

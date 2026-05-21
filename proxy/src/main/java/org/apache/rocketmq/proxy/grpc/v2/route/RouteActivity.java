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
package org.apache.rocketmq.proxy.grpc.v2.route;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class RouteActivity extends AbstractMessingActivity {

    public RouteActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        try {
            validateTopic(request.getTopic());
            // proxy address
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());

            String topicName = request.getTopic().getName();
            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx, addressList, topicName);
            // 所有副本集下所有 broker 中的所有 messageQueue
            List<MessageQueue> messageQueueList = new ArrayList<>();
            // brokerName -> brokerId -> gRPC Broker, Endpoints 为 proxyAddressList
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());
            // 获取 topic 的 messageType
            TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(ctx, topicName);
            // topicQueueTable 中 topic 下的所有 QueueData（所有副本集）
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                String brokerName = queueData.getBrokerName();
                // brokerId -> gRPC Broker, Endpoints 为 proxyAddressList
                Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
                if (brokerIdMap == null) {
                    break;
                }
                // 副本集下所有的 broker
                for (Broker broker : brokerIdMap.values()) {
                    // 构建某一个 broker 中的所有 messageQueue
                    messageQueueList.addAll(this.genMessageQueueFromQueueData(queueData, request.getTopic(), topicMessageType, broker));
                }
            }

            QueryRouteResponse response = QueryRouteResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .addAllMessageQueues(messageQueueList)
                .build();
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            // 客户端中配置的 Endpoints，也就是 proxy 地址
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());
            // 获取 topic 路由，路由中 brokerData 中的地址会全部被替换为 proxy 地址
            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx,
                addressList,
                request.getTopic().getName());

            boolean fifo = false;
            // 获取 consuemrGroup 的 SubscriptionGroupConfig（消费者本地配置和远程 broker 配置合并）
            SubscriptionGroupConfig config = this.messagingProcessor.getSubscriptionGroupConfig(ctx,
                request.getGroup().getName());
            if (config != null && config.isConsumeMessageOrderly()) {
                fifo = true;
            }
            // 一个 Assignment 对应一个 MessageQueue
            List<Assignment> assignments = new ArrayList<>();
            // BrokerName（副本组名称） -> BrokerId
            // topic 所在的所有 broker
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(proxyTopicRouteData.getBrokerDatas());
            // topicQueueTable 中 topic 下的所有 QueueData（副本集维度）
            // 副本集中的 queue 信息
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                // 过滤可读 queue
                if (PermName.isReadable(queueData.getPerm()) && queueData.getReadQueueNums() > 0) {
                    // 队列所在的副本组
                    Map<Long, Broker> brokerIdMap = brokerMap.get(queueData.getBrokerName());
                    if (brokerIdMap != null) {
                        // 该副本组的 master 节点
                        Broker broker = brokerIdMap.get(MixAll.MASTER_ID);
                        Permission permission = this.convertToPermission(queueData.getPerm());
                        // 如果是 fifo 则收集所有副本集中的所有 queue
                        if (fifo) {
                            // 副本集中的所有可读队列
                            for (int i = 0; i < queueData.getReadQueueNums(); i++) {
                                MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                    .setTopic(request.getTopic())
                                    .setId(i) // 确定的 queueId
                                    .setPermission(permission)
                                    .setBroker(broker) // master
                                    .build();
                                assignments.add(Assignment.newBuilder()
                                    .setMessageQueue(defaultMessageQueue)
                                    .build());
                            }
                        } else {
                            // 非 fifo 则每个副本集只收集一个 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
                            MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                .setTopic(request.getTopic())
                                .setId(-1) // 后续再 broker 随机选择 queue
                                .setPermission(permission)
                                .setBroker(broker) // 队列所在副本组的 master 节点
                                .build();
                            assignments.add(Assignment.newBuilder()
                                .setMessageQueue(defaultMessageQueue)
                                .build());
                        }

                    }
                }
            }

            QueryAssignmentResponse response;
            if (assignments.isEmpty()) {
                response = QueryAssignmentResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.FORBIDDEN, "no readable queue"))
                    .build();
            } else {
                response = QueryAssignmentResponse.newBuilder()
                    .addAllAssignments(assignments)
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                    .build();
            }
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected Permission convertToPermission(int perm) {
        boolean isReadable = PermName.isReadable(perm);
        boolean isWriteable = PermName.isWriteable(perm);
        if (isReadable && isWriteable) {
            return Permission.READ_WRITE;
        }
        if (isReadable) {
            return Permission.READ;
        }
        if (isWriteable) {
            return Permission.WRITE;
        }
        return Permission.NONE;
    }

    protected List<org.apache.rocketmq.proxy.common.Address> convertToAddressList(Endpoints endpoints) {
        // false
        boolean useEndpointPort = ConfigurationManager.getProxyConfig().isUseEndpointPortFromRequest();

        List<org.apache.rocketmq.proxy.common.Address> addressList = new ArrayList<>();
        for (Address address : endpoints.getAddressesList()) {
            int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
            if (useEndpointPort) {
                port = address.getPort();
            }
            addressList.add(new org.apache.rocketmq.proxy.common.Address(
                org.apache.rocketmq.proxy.common.Address.AddressScheme.valueOf(endpoints.getScheme().name()),
                HostAndPort.fromParts(address.getHost(), port)));
        }

        return addressList;

    }

    protected Map<String /*brokerName*/, Map<Long /*brokerID*/, Broker>> buildBrokerMap(
        List<ProxyTopicRouteData.ProxyBrokerData> brokerDataList) {
        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();
        // topicQueueTable 中 topic 下所有副本集 BrokerData
        // 但是其中的 BrokerAddrs 发生了替换 key 依然是 brokerId, 但是 brokerAddr 变成 proxyAddressList
        for (ProxyTopicRouteData.ProxyBrokerData brokerData : brokerDataList) {
            Map<Long, Broker> brokerIdMap = new HashMap<>();
            String brokerName = brokerData.getBrokerName();
            for (Map.Entry<Long, List<org.apache.rocketmq.proxy.common.Address>> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                // 转换为 gRPC  Address
                List<Address> addressList = new ArrayList<>();
                AddressScheme addressScheme = AddressScheme.IPv4;
                // proxyAddressList
                for (org.apache.rocketmq.proxy.common.Address address : entry.getValue()) {
                    addressScheme = AddressScheme.valueOf(address.getAddressScheme().name());
                    addressList.add(Address.newBuilder()
                        .setHost(address.getHostAndPort().getHost())
                        .setPort(address.getHostAndPort().getPort())
                        .build());
                }
                // gRPC Broker, Endpoints 为 proxyAddressList
                Broker broker = Broker.newBuilder()
                    .setName(brokerName)
                    .setId(Math.toIntExact(brokerId))
                    .setEndpoints(Endpoints.newBuilder()
                        .setScheme(addressScheme)
                        .addAllAddresses(addressList)
                        .build())
                    .build();

                brokerIdMap.put(brokerId, broker);
            }
            brokerMap.put(brokerName, brokerIdMap);
        }
        return brokerMap;
    }
    // QueueData 是按照副本集的粒度来划分的，Broker 是该副本集下的某一个 broker
    protected List<MessageQueue> genMessageQueueFromQueueData(QueueData queueData, Resource topic,
        TopicMessageType topicMessageType, Broker broker) {
        List<MessageQueue> messageQueueList = new ArrayList<>();
        // readOnly queue nums
        int r = 0;
        // writeOnly queue nums
        int w = 0;
        // both readable and writable queue nums.
        int rw = 0;
        int n = 0;
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            r = queueData.getReadQueueNums() - rw;
            w = queueData.getWriteQueueNums() - rw;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            w = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            r = queueData.getReadQueueNums();
        } else if (!PermName.isAccessible(queueData.getPerm())) {
            n = Math.max(1, Math.max(queueData.getWriteQueueNums(), queueData.getReadQueueNums()));
        }

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
        int queueIdIndex = 0;
        for (int i = 0; i < r; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < w; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < rw; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ_WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < n; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.NONE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        return messageQueueList;
    }

    private List<MessageType> parseTopicMessageType(TopicMessageType topicMessageType) {
        switch (topicMessageType) {
            case NORMAL:
                return Collections.singletonList(MessageType.NORMAL);
            case FIFO:
                return Collections.singletonList(MessageType.FIFO);
            case TRANSACTION:
                return Collections.singletonList(MessageType.TRANSACTION);
            case DELAY:
                return Collections.singletonList(MessageType.DELAY);
            case MIXED:
                return Arrays.asList(MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION);
            default:
                return Collections.singletonList(MessageType.MESSAGE_TYPE_UNSPECIFIED);
        }
    }
}

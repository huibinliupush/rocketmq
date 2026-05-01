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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;

public class MessageQueueSelector {
    private static final int BROKER_ACTING_QUEUE_ID = -1;

    // multiple queues for brokers with queueId : normal
    // 构建 topic 下所有副本集中 master 上的所有可读或者可写队列
    // 每个队列中的 queueId 在副本集内部排序，brokerNameA 中有 queueId = 0，brokerNameB 中也有 queueId = 0，
    // queueId 是按照副本集的维度从 0 递增的
    // queue 排序优先级： topic->brokerName->queueId
    // 这里的 queue 需要定义严格明确的排序规则，因为后续选取 queue 的时候强烈依赖这里 queue 的顺序（随机，一致性哈希）
    private final List<AddressableMessageQueue> queues = new ArrayList<>();
    // one queue for brokers with queueId : -1
    // topic 下所有可写或者可读的 messageQueue, queueId = BROKER_ACTING_QUEUE_ID = -1
    // 注意这里的一个副本集 brokerName，对应一个 AddressableMessageQueue（queueId = -1）
    // 虽然副本集中可能包含多个 AddressableMessageQueue，但是这里只存一个 queueId 为 -1
    private final List<AddressableMessageQueue> brokerActingQueues = new ArrayList<>();
    // 一个副本集 brokerName，对应一个 AddressableMessageQueue（queueId = -1）
    private final Map<String, AddressableMessageQueue> brokerNameQueueMap = new ConcurrentHashMap<>();
    // 可能是负数,用于从 queues 中选取 messageQueue
    private final AtomicInteger queueIndex;
    // 可能是负数,用于从 brokerActingQueues 选取 messageQueue
    private final AtomicInteger brokerIndex;
    // org.apache.rocketmq.proxy.service.route.TopicRouteService.mqFaultStrategy
    private MQFaultStrategy mqFaultStrategy;

    public MessageQueueSelector(TopicRouteWrapper topicRouteWrapper, MQFaultStrategy mqFaultStrategy, boolean read) {
        if (read) {
            // 构建 topic 下所有副本集中 master 上的可读队列
            // 每个队列中的 queueId 在副本集内部排序，brokerNameA 中有 queueId = 0，brokerNameB 中也有 queueId = 0，
            // queue 排序优先级： topic->brokerName->queueId
            this.queues.addAll(buildRead(topicRouteWrapper));
        } else {
            // queueId 是按照副本集的维度从 0 递增的
            this.queues.addAll(buildWrite(topicRouteWrapper));
        }
        buildBrokerActingQueues(topicRouteWrapper.getTopicName(), this.queues);
        Random random = new Random();
        /**
         * nextInt()：返回一个 int 范围内的伪随机数（包括负数）。一个伪随机的 int 值，均匀分布在 -2^31 到 2^31-1（即 int 的全部取值范围）之间
         *
         * nextInt(int bound)：返回 [0, bound) 之间的伪随机整数（0 包含，bound 不包含）。[0, bound) 之间的伪随机 int 值，每个值出现的概率近似相等
         * bound 必须为正数，大于 0
         *
         * Random 基于线性同余生成器（LCG），可预测。不适合安全敏感场景。
         *
         * 相同种子（seed）生成的随机数序列完全相同
         * Random 实例是线程安全的（内部使用 AtomicLong 更新种子），但多线程下会有竞争，性能较低。线程安全但有锁竞争
         *
         * 高并发场景推荐使用 ThreadLocalRandom.current().nextInt(bound),每个线程独立种子，无竞争，性能极高
         *
         * 无参构造 new Random() 使用系统纳秒时间作为种子，通常足够随机。
         *
         * 需要可重现的序列时，可指定种子：new Random(12345L)
         * */
        this.queueIndex = new AtomicInteger(random.nextInt());// 可能是负数
        this.brokerIndex = new AtomicInteger(random.nextInt());// 可能是负数
        this.mqFaultStrategy = mqFaultStrategy;
    }

    private static List<AddressableMessageQueue> buildRead(TopicRouteWrapper topicRoute) {
        Set<AddressableMessageQueue> queueSet = new HashSet<>();
        // topicQueueTable 中 topic 下的所有 QueueData（所有副本集master上的队列）
        List<QueueData> qds = topicRoute.getQueueDatas();
        if (qds == null) {
            return new ArrayList<>();
        }
        // QueueData 为某个副本集下的 messageQueue 信息
        for (QueueData qd : qds) {
            // messageQueue 有可读权限
            if (PermName.isReadable(qd.getPerm())) {
                // messageQueue 所在副本集的 master address
                String brokerAddr = topicRoute.getMasterAddrPrefer(qd.getBrokerName());
                if (brokerAddr == null) {
                    // 如果副本集中没有 master 则跳过
                    continue;
                }

                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    // queueId 按照副本集排序
                    // brokerNameA 中有 queueId = 0，brokerNameB 中也有 queueId = 0，
                    AddressableMessageQueue mq = new AddressableMessageQueue(
                        new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                        brokerAddr);
                    queueSet.add(mq);
                }
            }
        }
        // 排序优先级： topic->brokerName->queueId
        // 这里的 queue 需要定义严格明确的排序规则，因为后续选取 queue 的时候强烈依赖这里 queue 的顺序（随机，一致性哈希）
        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private static List<AddressableMessageQueue> buildWrite(TopicRouteWrapper topicRoute) {
        Set<AddressableMessageQueue> queueSet = new HashSet<>();
        // order topic route.
        if (StringUtils.isNotBlank(topicRoute.getOrderTopicConf())) {
            String[] brokers = topicRoute.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                // 副本集
                String brokerName = item[0];
                // 副本集 master address
                String brokerAddr = topicRoute.getMasterAddr(brokerName);
                if (brokerAddr == null) {
                    continue;
                }
                // 副本集中 writable queue num
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    // queueId 是按照副本集的维度从 0 递增的
                    AddressableMessageQueue mq = new AddressableMessageQueue(
                        new MessageQueue(topicRoute.getTopicName(), brokerName, i),
                        brokerAddr);
                    queueSet.add(mq);
                }
            }
        } else {
            List<QueueData> qds = topicRoute.getQueueDatas();
            if (qds == null) {
                return new ArrayList<>();
            }

            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    String brokerAddr = topicRoute.getMasterAddr(qd.getBrokerName());
                    if (brokerAddr == null) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        AddressableMessageQueue mq = new AddressableMessageQueue(
                            new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                            brokerAddr);
                        queueSet.add(mq);
                    }
                }
            }
        }
        // 这里的 queue 需要定义严格明确的排序规则，因为后续选取 queue 的时候强烈依赖这里 queue 的顺序（随机，一致性哈希）
        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private void buildBrokerActingQueues(String topic, List<AddressableMessageQueue> normalQueues) {
        // topic 下所有副本集中 master 上的所有可写或者可读的 messageQueue
        for (AddressableMessageQueue mq : normalQueues) {
            AddressableMessageQueue brokerActingQueue = new AddressableMessageQueue(
                new MessageQueue(topic, mq.getMessageQueue().getBrokerName(), BROKER_ACTING_QUEUE_ID),
                mq.getBrokerAddr());

            if (!brokerActingQueues.contains(brokerActingQueue)) {
                brokerActingQueues.add(brokerActingQueue);
                brokerNameQueueMap.put(brokerActingQueue.getBrokerName(), brokerActingQueue);
            }
        }

        Collections.sort(brokerActingQueues);
    }

    public AddressableMessageQueue getQueueByBrokerName(String brokerName) {
        return this.brokerNameQueueMap.get(brokerName);
    }

    public AddressableMessageQueue selectOne(boolean onlyBroker) {
        int nextIndex = onlyBroker ? brokerIndex.getAndIncrement() : queueIndex.getAndIncrement();
        // 通过 queueIndex 轮询选取 messageQueue (所有副本集中的所有 messageQueue)
        return selectOneByIndex(nextIndex, onlyBroker);
    }

    public AddressableMessageQueue selectOneByPipeline(boolean onlyBroker) {
        // false
        if (mqFaultStrategy != null && mqFaultStrategy.isSendLatencyFaultEnable()) {
            List<MessageQueue> messageQueueList = null;
            MessageQueue messageQueue = null;
            if (onlyBroker) {
                messageQueueList = transferAddressableQueues(brokerActingQueues);
            } else {
                messageQueueList = transferAddressableQueues(queues);
            }
            AddressableMessageQueue addressableMessageQueue = null;

            // use both available filter.
            messageQueue = selectOneMessageQueue(messageQueueList, onlyBroker ? brokerIndex : queueIndex,
                    mqFaultStrategy.getAvailableFilter(), mqFaultStrategy.getReachableFilter());
            addressableMessageQueue = transferQueue2Addressable(messageQueue);
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }

            // use available filter.
            messageQueue = selectOneMessageQueue(messageQueueList, onlyBroker ? brokerIndex : queueIndex,
                    mqFaultStrategy.getAvailableFilter());
            addressableMessageQueue = transferQueue2Addressable(messageQueue);
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }

            // no available filter, then use reachable filter.
            messageQueue = selectOneMessageQueue(messageQueueList, onlyBroker ? brokerIndex : queueIndex,
                    mqFaultStrategy.getReachableFilter());
            addressableMessageQueue = transferQueue2Addressable(messageQueue);
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }
        }

        // SendLatency is not enabled, or no queue is selected, then select by index.
        // 通过 queueIndex 轮询选取 messageQueue (所有副本集中的所有 messageQueue)
        return selectOne(onlyBroker);
    }

    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, AtomicInteger sendQueue, TopicPublishInfo.QueueFilter...filter) {
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }
        if (filter != null && filter.length != 0) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                for (TopicPublishInfo.QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }
        }
        return null;
    }

    public List<MessageQueue> transferAddressableQueues(List<AddressableMessageQueue> addressableMessageQueueList) {
        if (addressableMessageQueueList == null) {
            return null;
        }

        return addressableMessageQueueList.stream()
                .map(AddressableMessageQueue::getMessageQueue)
                .collect(Collectors.toList());
    }

    private AddressableMessageQueue transferQueue2Addressable(MessageQueue messageQueue) {
        for (AddressableMessageQueue amq: queues) {
            if (amq.getMessageQueue().equals(messageQueue)) {
                return amq;
            }
        }
        return null;
    }

    public AddressableMessageQueue selectNextOne(AddressableMessageQueue last) {
        boolean onlyBroker = last.getQueueId() < 0;
        AddressableMessageQueue newOne = last;
        int count = onlyBroker ? brokerActingQueues.size() : queues.size();

        for (int i = 0; i < count; i++) {
            newOne = selectOne(onlyBroker);
            if (!newOne.getBrokerName().equals(last.getBrokerName()) || newOne.getQueueId() != last.getQueueId()) {
                break;
            }
        }
        return newOne;
    }

    public AddressableMessageQueue selectOneByIndex(int index, boolean onlyBroker) {
        if (onlyBroker) {
            if (brokerActingQueues.isEmpty()) {
                return null;
            }
            // 注意这里的一个副本集 brokerName，对应一个 AddressableMessageQueue（queueId = -1）
            // 虽然副本集中可能包含多个 AddressableMessageQueue，但是这里只存一个 queueId 为 -1
            // IntMath.mod 为了解决 Java 原生取模（%）运算符的“负数陷阱”而生的一种优雅解决方案。返回值始终非负
            // IntMath.mod 与 % 在处理负数时的不同：IntMath.mod(-7, 4) 得到的是 1，而 (-7) % 4 得到的是 -3
            return brokerActingQueues.get(IntMath.mod(index, brokerActingQueues.size()));
        }

        if (queues.isEmpty()) {
            return null;
        }
        // 构建 topic 下所有副本集中的可读或者可写队列
        // 每个队列中的 queueId 在副本集内部排序，brokerNameA 中有 queueId = 0，brokerNameB 中也有 queueId = 0，
        // queueId 是按照副本集的维度从 0 递增的
        // queue 排序优先级： topic->brokerName->queueId
        return queues.get(IntMath.mod(index, queues.size()));
    }

    public List<AddressableMessageQueue> getQueues() {
        return queues;
    }

    public List<AddressableMessageQueue> getBrokerActingQueues() {
        return brokerActingQueues;
    }

    public MQFaultStrategy getMQFaultStrategy() {
        return mqFaultStrategy;
    }

    public void setMQFaultStrategy(MQFaultStrategy mqFaultStrategy) {
        this.mqFaultStrategy = mqFaultStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageQueueSelector)) {
            return false;
        }
        MessageQueueSelector queue = (MessageQueueSelector) o;
        return Objects.equals(queues, queue.queues) &&
            Objects.equals(brokerActingQueues, queue.brokerActingQueues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queues, brokerActingQueues);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("queues", queues)
            .add("brokerActingQueues", brokerActingQueues)
            .add("brokerNameQueueMap", brokerNameQueueMap)
            .add("queueIndex", queueIndex)
            .add("brokerIndex", brokerIndex)
            .toString();
    }
}

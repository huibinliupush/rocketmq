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
package org.apache.rocketmq.common.sysflag;

public class TopicSysFlag {
    // 默认都是 false
    // 为了支持“单元化”（Unit）架构而设计的一种特殊 Topic。
    // 你可以把它理解成一种 “分域路由”机制，它的核心思想是为 Topic 增加一个“单元”（Unit）维度。通过它，
    // 消息可以被精准地路由到指定单元（通常是地理上隔离的数据中心或逻辑分区）的 Broker 中，从而在物理层面实现数据的强隔离。
    private final static int FLAG_UNIT = 0x1 << 0;

    /**
     *
     * Unit Subscription 是实现 RocketMQ 单元化架构的另一个关键部分。如果说 Unit Topic 解决了消息在单元间写入的路由问题，那么 Unit Subscription 则解决了消息在单元内消费的隔离问题。两者结合，才能构建一个完整的、读写分离的单元化消息传递模型，是实现数据强隔离和单元化架构的关键一环
     * (Unit Subscription) 这个概念就应运而生了。它是 “单元化订阅”的简称，是一种为支持单元化架构而设计的特殊订阅机制
     *
     * 在同一个“单元化消费组”（Unit Consumer Group）内，消费者实例被划分为若干“单元”，每个单元的消费者只会消费发往其所在单元的 Unit Topic 中的消息。
     *
     * 它的核心在于，将物理上分布在各个单元（数据中心或逻辑分区）的 Unit Topic 队列，在消费端进行逻辑隔离。消费者组内的每个消费者实例（或子组）都“钉”在了特定的单元上，只处理属于自己单元的数据
     *
     * 举个例子，假设我们有一个 Unit Topic 名为 order_topic，它在“上海单元”和“北京单元”各有 3 个队列。如果采用普通的订阅方式，一个跨地域的消费者组可能会从两个单元的所有 6 个队列中拉取消息，导致数据被跨单元消费。
     *
     * 而通过“单元化订阅”，我们可以创建两个单元消费组 order_consumer_group_sh 和 order_consumer_group_bj，并分别绑定到“上海单元”和“北京单元”。这样，两个单元的消费组在逻辑上完全隔离，确保了单元化消费的实现。
     * */
    private final static int FLAG_UNIT_SUB = 0x1 << 1;

    public static int buildSysFlag(final boolean unit, final boolean hasUnitSub) {
        int sysFlag = 0;

        if (unit) {
            sysFlag |= FLAG_UNIT;
        }

        if (hasUnitSub) {
            sysFlag |= FLAG_UNIT_SUB;
        }

        return sysFlag;
    }

    public static int setUnitFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT;
    }

    public static int clearUnitFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT);
    }

    public static boolean hasUnitFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT) == FLAG_UNIT;
    }

    public static int setUnitSubFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT_SUB;
    }

    public static int clearUnitSubFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT_SUB);
    }

    public static boolean hasUnitSubFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT_SUB) == FLAG_UNIT_SUB;
    }
}

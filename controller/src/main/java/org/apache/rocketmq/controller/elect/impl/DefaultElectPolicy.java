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
package org.apache.rocketmq.controller.elect.impl;

import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.helper.BrokerLiveInfoGetter;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultElectPolicy implements ElectPolicy {

    // <clusterName, brokerName, brokerAddr>, Used to judge whether a broker
    // has preliminary qualification to be selected as master
    // org.apache.rocketmq.controller.impl.manager.RaftReplicasInfoManager.isBrokerActive
    private BrokerValidPredicate validPredicate;

    // <clusterName, brokerName, brokerAddr, BrokerLiveInfo>, Used to obtain the BrokerLiveInfo information of a broker
    private BrokerLiveInfoGetter brokerLiveInfoGetter;

    // Sort in descending order according to<epoch, offset>, and sort in ascending order according to priority
    private final Comparator<BrokerLiveInfo> comparator = (o1, o2) -> {
        if (o1.getEpoch() == o2.getEpoch()) {
            return o1.getMaxOffset() == o2.getMaxOffset() ? o1.getElectionPriority() - o2.getElectionPriority() :
                (int) (o2.getMaxOffset() - o1.getMaxOffset());
        } else {
            return o2.getEpoch() - o1.getEpoch(); // epoch 大的优先
        }
    };
    // see : org.apache.rocketmq.controller.impl.JRaftControllerStateMachine.electMaster
    public DefaultElectPolicy(BrokerValidPredicate validPredicate, BrokerLiveInfoGetter brokerLiveInfoGetter) {
        // org.apache.rocketmq.controller.impl.manager.RaftReplicasInfoManager.isBrokerActive
        this.validPredicate = validPredicate;
        this.brokerLiveInfoGetter = brokerLiveInfoGetter;
    }

    public DefaultElectPolicy() {

    }

    /**
     * We will try to select a new master from syncStateBrokers and allReplicaBrokers in turn.
     * The strategies are as follows:
     *    - Filter alive brokers by 'validPredicate'.
     *    - Check whether the old master is still valid.
     *    - If preferBrokerAddr is not empty and valid, select it as master.
     *    - Otherwise, we will sort the array of 'brokerLiveInfo' according to (epoch, offset, electionPriority), and select the best candidate as the new master.
     *
     * @param clusterName       the brokerGroup belongs
     * @param syncStateBrokers  all broker replicas in syncStateSet
     * @param allReplicaBrokers all broker replicas
     * @param oldMaster         old master's broker id
     * @param preferBrokerId    the broker id prefer to be elected
     * @return master elected by our own policy
     */
    @Override
    public Long elect(String clusterName, String brokerName, Set<Long> syncStateBrokers, Set<Long> allReplicaBrokers,
        Long oldMaster, Long preferBrokerId) {
        Long newMaster = null;
        // try to elect in syncStateBrokers
        if (syncStateBrokers != null) {
            // 首先比较 broker 的 epoch , 选取最大的 epoch 为 master
            // 如果 epoch 相同，则继续看 MaxOffset ，选取最大的 maxOffset 为 master
            // 如果 epoch , maxOffset 都相同，则以 ElectionPriority 为准，值越小，越有机会成为 master
            // 如果全部相同，则选第一个
            newMaster = tryElect(clusterName, brokerName, syncStateBrokers, oldMaster, preferBrokerId);
        }
        if (newMaster != null) {
            return newMaster;
        }

        // 如果 EnableElectUncleanMaster ， try to elect in all allReplicaBrokers
        if (allReplicaBrokers != null) {
            newMaster = tryElect(clusterName, brokerName, allReplicaBrokers, oldMaster, preferBrokerId);
        }
        return newMaster;
    }

    private Long tryElect(String clusterName, String brokerName, Set<Long> brokers, Long oldMaster,
        Long preferBrokerId) {
        // org.apache.rocketmq.controller.impl.manager.RaftReplicasInfoManager.isBrokerActive
        if (this.validPredicate != null) {
            // 首先在备选集合中，过滤出所有 active 的 broker
            brokers = brokers.stream().filter(brokerAddr -> this.validPredicate.check(clusterName, brokerName, brokerAddr)).collect(Collectors.toSet());
        }
        if (!brokers.isEmpty()) {
            // if old master is still valid, and preferBrokerAddr is blank or is equals to oldMaster
            // 如果 old master 依旧存活，那么还是原来的 master
            // 但如果我们指定了 preferBrokerId ， 那么就以指定的为准
            if (brokers.contains(oldMaster) && (preferBrokerId == null || preferBrokerId.equals(oldMaster))) {
                return oldMaster;
            }

            // if preferBrokerAddr is valid, we choose it, otherwise we choose nothing
            if (preferBrokerId != null) {
                return brokers.contains(preferBrokerId) ? preferBrokerId : null;
            }

            if (this.brokerLiveInfoGetter != null) {
                // sort brokerLiveInfos by (epoch,maxOffset)
                // 首先比较 broker 的 epoch , 选取最大的 epoch 为 master
                // 如果 epoch 相同，则继续看 MaxOffset ，选取最大的 maxOffset 为 master
                // 如果 epoch , maxOffset 都相同，则以 ElectionPriority 为准，值越小，越有机会成为 master
                // 如果全部相同，则选第一个
                TreeSet<BrokerLiveInfo> brokerLiveInfos = new TreeSet<>(this.comparator);
                // 挨个获取备选集合中 acitve broker 的 BrokerLiveInfo，并加入到 brokerLiveInfos 中
                brokers.forEach(brokerAddr -> brokerLiveInfos.add(this.brokerLiveInfoGetter.get(clusterName, brokerName, brokerAddr)));
                if (brokerLiveInfos.size() >= 1) {
                    return brokerLiveInfos.first().getBrokerId();
                }
            }
            // elect random
            return brokers.iterator().next();
        }
        return null;
    }


    public void setBrokerLiveInfoGetter(BrokerLiveInfoGetter brokerLiveInfoGetter) {
        this.brokerLiveInfoGetter = brokerLiveInfoGetter;
    }

    public void setValidPredicate(BrokerValidPredicate validPredicate) {
        this.validPredicate = validPredicate;
    }

    public BrokerLiveInfoGetter getBrokerLiveInfoGetter() {
        return brokerLiveInfoGetter;
    }
}

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
package org.apache.rocketmq.controller.impl;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.closure.ControllerClosure;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.manager.RaftReplicasInfoManager;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetSyncStateDataRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.controller.metrics.ControllerMetricsConstant;
import org.apache.rocketmq.controller.metrics.ControllerMetricsManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_BROKER_SET;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_ELECTION_RESULT;

public class JRaftControllerStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final List<Consumer<Long>> onLeaderStartCallbacks;
    private final List<Consumer<Status>> onLeaderStopCallbacks;
    // broker 的选主是从 syncStateSetInfoTable 中挑选一个 slave 作为 leader
    // 所以为了 broker 选主的一致性。必须用 raft 协议保证 syncStateSetInfoTable 的一致性（通过 jraft 保证）
    // 因此状态机中保存的数据是 replicasInfoManager，在 leader controller 挂的时候，最起码能保证其他 controller 节点上的
    // replicasInfoManager 是一致的，近而通过 replicasInfoManager 选取出的 master broker 也是一致的
    private final RaftReplicasInfoManager replicasInfoManager; // broker 选主的元数据，需要通过 raft 来保证一致性（在各个 controller 节点）
    private final NodeId nodeId;

    public JRaftControllerStateMachine(ControllerConfig controllerConfig, NodeId nodeId) {
        this.replicasInfoManager = new RaftReplicasInfoManager(controllerConfig);
        this.nodeId = nodeId;
        this.onLeaderStartCallbacks = new ArrayList<>();
        this.onLeaderStopCallbacks = new ArrayList<>();
    }

    /**
     * node.apply 提交的任务最终将会复制应用到所有 raft 节点上的状态机
     * 应用任务列表到状态机，任务将按照提交顺序应用。请注意，当这个方法返回的时候，我们就认为这一批任务都已经成功应用到状态机上，
     * 如果你没有完全应用（比如错误、异常），
     * 将会被当做一个 critical 级别的错误，报告给状态机的 onError 方法，错误类型为 ERROR_TYPE_STATE_MACHINE
     *
     * 向 raft 节点状态机提交任务 - node.apply
     * see : org.apache.rocketmq.controller.impl.JRaftController#applyToJRaft(org.apache.rocketmq.remoting.protocol.RemotingCommand)
     *
     * 各个 raft 节点通过  node.apply 提交的任务都会通过 raft 协议记录在各个节点的 raft log 中，log 在各个 raft 节点中是一致的
     * 状态机通过 log 一个一个的 apply 到状态机中，log 是一致的，所以各个 raft 节点状态机中保存的状态也是一直的
     * 比如，在一个 raft group 中维护一个计数器，各个 raft 节点可以更改这个计数器的值，并且这个值对各个 raft 节点是一致的
     * log : +1 ,+1 ,+1  , log 在各个节点中一致，那么各节点状态机中这个计数器的值就都是 3
     *
     * 再比如 rocketmq 这里， log 记录的就是 broker 的 request : request1,request2,request3,request4
     * 那么对于各个 raft 节点来说，只需要按照 log , 挨个调用这里的 apply 方法，将 log 中的 request 重放
     * 那么各个 raft 节点状态机中保存的数据结构就是一致的
     * */
    @Override
    public void onApply(Iterator iter) {
        // iter 为 node 提交的 task
        while (iter.hasNext()) {
            // node 提交的 RemotingCommand
            byte[] data = iter.getData().array();
            // task 完成后的回调函数
            ControllerClosure controllerClosure = (ControllerClosure) iter.done();
            // iter.getIndex() : log index 提交到 raft group 中的任务都将序列化为一条日志存储下来，每条日志一个编号，
            // 在整个 raft group 内单调递增并复制到每个 raft 节点。

            // iter.getTerm() : term 在整个 raft group 中单调递增的一个 long 数字，可以简单地认为表示一轮投票的编号，
            // 成功选举出来的 leader 对应的 term 称为 leader term，在这个 leader 没有发生变更的阶段内提交的日志都将拥有相同的 term 编号。
            processEvent(controllerClosure, data, iter.getTerm(), iter.getIndex());

            iter.next();
        }
    }

    private void processEvent(ControllerClosure controllerClosure, byte[] data, long term, long index) {
        RemotingCommand request;
        ControllerResult<?> result;
        try {
            // 本机 raft 节点 node 提交的 task
            if (controllerClosure != null) {
                // 无需序列化
                request = controllerClosure.getRequestEvent();
            } else {
                // 其他 raft 节点提交的 task 需要序列化
                request = RemotingCommand.decode(Arrays.copyOfRange(data, 4, data.length));
            }
            log.info("process event: term {}, index {}, request code {}", term, index, request.getCode());
            switch (request.getCode()) {
                case RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET:
                    AlterSyncStateSetRequestHeader requestHeader = (AlterSyncStateSetRequestHeader) request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
                    SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
                    result = alterSyncStateSet(requestHeader, syncStateSet);
                    break;
                case RequestCode.CONTROLLER_ELECT_MASTER:
                    ElectMasterRequestHeader electMasterRequestHeader = (ElectMasterRequestHeader) request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
                    // 首先比较 broker 的 epoch , 选取最大的 epoch 为 master
                    // 如果 epoch 相同，则继续看 MaxOffset ，选取最大的 maxOffset 为 master
                    // 如果 epoch , maxOffset 都相同，则以 ElectionPriority 为准，值越小，越有机会成为 master
                    // 如果全部相同，则选第一个
                    result = electMaster(electMasterRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_NEXT_BROKER_ID:
                    GetNextBrokerIdRequestHeader getNextBrokerIdRequestHeader = (GetNextBrokerIdRequestHeader) request.decodeCommandCustomHeader(GetNextBrokerIdRequestHeader.class);
                    result = getNextBrokerId(getNextBrokerIdRequestHeader);
                    break;
                case RequestCode.CONTROLLER_APPLY_BROKER_ID:
                    ApplyBrokerIdRequestHeader applyBrokerIdRequestHeader = (ApplyBrokerIdRequestHeader) request.decodeCommandCustomHeader(ApplyBrokerIdRequestHeader.class);
                    result = applyBrokerId(applyBrokerIdRequestHeader);
                    break;
                case RequestCode.CONTROLLER_REGISTER_BROKER:
                    RegisterBrokerToControllerRequestHeader registerBrokerToControllerRequestHeader = (RegisterBrokerToControllerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
                    result = registerBroker(registerBrokerToControllerRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_REPLICA_INFO:
                    GetReplicaInfoRequestHeader getReplicaInfoRequestHeader = (GetReplicaInfoRequestHeader) request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
                    result = getReplicaInfo(getReplicaInfoRequestHeader);
                    break;
                case RequestCode.CONTROLLER_GET_SYNC_STATE_DATA:
                    List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
                    GetSyncStateDataRequest getSyncStateDataRequest = (GetSyncStateDataRequest) request.decodeCommandCustomHeader(GetSyncStateDataRequest.class);
                    result = getSyncStateData(brokerNames, getSyncStateDataRequest.getInvokeTime());
                    break;
                case RequestCode.CLEAN_BROKER_DATA:
                    CleanControllerBrokerDataRequestHeader cleanBrokerDataRequestHeader = (CleanControllerBrokerDataRequestHeader) request.decodeCommandCustomHeader(CleanControllerBrokerDataRequestHeader.class);
                    result = cleanBrokerData(cleanBrokerDataRequestHeader);
                    break;
                case RequestCode.GET_BROKER_LIVE_INFO_REQUEST:
                    GetBrokerLiveInfoRequest getBrokerLiveInfoRequest = (GetBrokerLiveInfoRequest) request.decodeCommandCustomHeader(GetBrokerLiveInfoRequest.class);
                    result = replicasInfoManager.getBrokerLiveInfo(getBrokerLiveInfoRequest);
                    break;
                case RequestCode.RAFT_BROKER_HEART_BEAT_EVENT_REQUEST:
                    RaftBrokerHeartBeatEventRequest brokerHeartbeatRequestHeader = (RaftBrokerHeartBeatEventRequest) request.decodeCommandCustomHeader(RaftBrokerHeartBeatEventRequest.class);
                    result = replicasInfoManager.onBrokerHeartBeat(brokerHeartbeatRequestHeader);
                    break;
                case RequestCode.BROKER_CLOSE_CHANNEL_REQUEST:
                    BrokerCloseChannelRequest brokerCloseChannelRequest = (BrokerCloseChannelRequest) request.decodeCommandCustomHeader(BrokerCloseChannelRequest.class);
                    result = replicasInfoManager.onBrokerCloseChannel(brokerCloseChannelRequest);
                    break;
                case RequestCode.CHECK_NOT_ACTIVE_BROKER_REQUEST:
                    CheckNotActiveBrokerRequest checkNotActiveBrokerRequest = (CheckNotActiveBrokerRequest) request.decodeCommandCustomHeader(CheckNotActiveBrokerRequest.class);
                    result = replicasInfoManager.checkNotActiveBroker(checkNotActiveBrokerRequest);
                    break;
                default:
                    throw new RemotingCommandException("Unknown request code: " + request.getCode());
            }
            // ApplyBrokerIdEvent
            result.getEvents().forEach(replicasInfoManager::applyEvent);
        } catch (RemotingCommandException e) {
            log.error("Fail to process event", e);
            if (controllerClosure != null) {
                controllerClosure.run(new Status(RaftError.EINTERNAL, e.getMessage()));
            }
            return;
        }
        log.info("process event: term {}, index {}, request code {} success with result {}", term, index, request.getCode(), result.toString());
        if (controllerClosure != null) {
            controllerClosure.setControllerResult(result);
            controllerClosure.run(Status.OK());
        }
    }

    private ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(
        AlterSyncStateSetRequestHeader requestHeader, SyncStateSet syncStateSet) {
        return replicasInfoManager.alterSyncStateSet(requestHeader, syncStateSet, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), this.replicasInfoManager));
    }

    private ControllerResult<ElectMasterResponseHeader> electMaster(ElectMasterRequestHeader request) {
        // 首先比较 broker 的 epoch , 选取最大的 epoch 为 master
        // 如果 epoch 相同，则继续看 MaxOffset ，选取最大的 maxOffset 为 master
        // 如果 epoch , maxOffset 都相同，则以 ElectionPriority 为准，值越小，越有机会成为 master
        // 如果全部相同，则选第一个
        ControllerResult<ElectMasterResponseHeader> electResult = this.replicasInfoManager.electMaster(request, new DefaultElectPolicy(
            (clusterName, brokerName, brokerId) -> replicasInfoManager.isBrokerActive(clusterName, brokerName, brokerId, request.getInvokeTime()),
            replicasInfoManager::getBrokerLiveInfo
        ));
        log.info("elect master, request :{}, result: {}", request.toString(), electResult.toString());
        AttributesBuilder attributesBuilder = ControllerMetricsManager.newAttributesBuilder()
            .put(LABEL_CLUSTER_NAME, request.getClusterName())
            .put(LABEL_BROKER_SET, request.getBrokerName());
        switch (electResult.getResponseCode()) {
            case ResponseCode.SUCCESS:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.NEW_MASTER_ELECTED.getLowerCaseName()).build());
                break;
            case ResponseCode.CONTROLLER_MASTER_STILL_EXIST:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.KEEP_CURRENT_MASTER.getLowerCaseName()).build());
                break;
            case ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE:
            case ResponseCode.CONTROLLER_ELECT_MASTER_FAILED:
                ControllerMetricsManager.electionTotal.add(1,
                    attributesBuilder.put(LABEL_ELECTION_RESULT, ControllerMetricsConstant.ElectionResult.NO_MASTER_ELECTED.getLowerCaseName()).build());
                break;
            default:
                break;
        }
        return electResult;
    }

    private ControllerResult<GetNextBrokerIdResponseHeader> getNextBrokerId(
        GetNextBrokerIdRequestHeader requestHeader) {
        return replicasInfoManager.getNextBrokerId(requestHeader);
    }

    private ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerId(ApplyBrokerIdRequestHeader requestHeader) {
        return replicasInfoManager.applyBrokerId(requestHeader);
    }

    private ControllerResult<?> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        return replicasInfoManager.registerBroker(request, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(request.getInvokeTime(), this.replicasInfoManager));
    }

    private ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        return replicasInfoManager.getReplicaInfo(request);
    }

    private ControllerResult<Void> getSyncStateData(List<String> brokerNames, long invokeTile) {
        return replicasInfoManager.getSyncStateData(brokerNames, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(invokeTile, this.replicasInfoManager));
    }

    private ControllerResult<Void> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
        return replicasInfoManager.cleanBrokerData(requestHeader, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), this.replicasInfoManager));
    }

    /**
     * 当状态机所在 raft 节点被关闭的时候调用，可以用于一些状态机的资源清理工作，比如关闭文件等。
     * */
    @Override
    public void onShutdown() {
        log.info("StateMachine {} node {} onShutdown", getClass().getName(), nodeId.toString());
    }

    /**
     * Snapshot 的保存和加载
     * 保存状态的最新状态，保存的文件信息可以写到 SnapshotWriter 中，保存完成切记调用 done.run(status) 方法。
     * 通常情况下，每次 `onSnapshotSave` 被调用都应该阻塞状态机（同步调用）以保证用户可以捕获当前状态机的状态，如果想通过异步 snapshot 来提升性能，
     * 那么需要用户状态机支持快照读，并先同步读快照，再异步保存快照数据。
     * */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        byte[] data;
        try {
            data = this.replicasInfoManager.serialize();
        } catch (Throwable e) {
            done.run(new Status(RaftError.EIO, "Fail to serialize replicasInfoManager state machine data"));
            return;
        }
        Utils.runInThread(() -> {
            try {
                FileUtils.writeByteArrayToFile(new File(writer.getPath() + File.separator + "data"), data);
                if (writer.addFile("data")) {
                    log.info("Save snapshot, path={}", writer.getPath());
                    done.run(Status.OK());
                } else {
                    throw new IOException("Fail to add file to writer");
                }
            } catch (IOException e) {
                log.error("Fail to save snapshot", e);
                done.run(new Status(RaftError.EIO, "Fail to save snapshot"));
            }
        });
    }

    /**
     * 加载或者安装 snapshot，从 SnapshotReader 读取 snapshot 文件列表并使用。
     * 需要注意的是:
     *    程序启动会调用 `onSnapshotLoad` 方法，也就是说业务状态机的数据一致性保障全权由 jraft 接管，业务状态机的启动时应保持状态为空，
     *    如果状态机持久化了数据那么应该在启动时先清除数据，并依赖 raft snapshot + replay raft log 来恢复状态机数据。
     * */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        if (reader.getFileMeta("data") == null) {
            log.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        try {
            byte[] data = FileUtils.readFileToByteArray(new File(reader.getPath() + File.separator + "data"));
            this.replicasInfoManager.deserializeFrom(data);
            log.info("Load snapshot from {}", reader.getPath());
            return true;
        } catch (Throwable e) {
            log.error("Fail to load snapshot from {}", reader.getPath(), e);
            return false;
        }
    }

    /**
     * 状态机所属的 raft 节点成为 leader 的时候被调用，成为 leader 当前的 term 通过参数传入。
     * */
    @Override
    public void onLeaderStart(long term) {
        for (Consumer<Long> callback : onLeaderStartCallbacks) {
            callback.accept(term);
        }
        log.info("node {} Start Leader, term={}", nodeId.toString(), term);
    }
    /**
     * 当前状态机所属的 raft 节点失去 leader 资格时调用，status 字段描述了详细的原因，比如主动转移 leadership、重新发生选举等。
     * */
    @Override
    public void onLeaderStop(Status status) {
        for (Consumer<Status> callback : onLeaderStopCallbacks) {
            callback.accept(status);
        }
        log.info("node {} Stop Leader, status={}", nodeId.toString(), status);
    }

    public void registerOnLeaderStart(Consumer<Long> callback) {
        onLeaderStartCallbacks.add(callback);
    }

    public void registerOnLeaderStop(Consumer<Status> callback) {
        onLeaderStopCallbacks.add(callback);
    }

    /**
     *  critical 错误发生的时候，会调用此方法，RaftException 包含了 status 等详细的错误信息；
     *  当这个方法被调用后，将不允许新的任务应用到状态机，直到错误被修复并且节点被重启
     * */
    @Override
    public void onError(RaftException e) {
        log.error("Encountered an error={} on StateMachine {}, node {}, raft may stop working since some error occurs, you should figure out the cause and repair or remove this node.", e.getStatus(), this.getClass().getName(), nodeId.toString(), e);
    }

    /**
     * 当一个 raft group 的节点配置提交到 raft group 日志的时候调用，通常不需要实现此方法，或者打印个日志即可。
     * */
    @Override
    public void onConfigurationCommitted(Configuration conf) {
        log.info("Configuration committed, conf={}", conf);
    }

    /**
     * 当一个 raft follower 停止 follower 一个 leader 节点的时候调用，这种情况一般是发生了 leadership 转移，
     * 比如重新选举产生了新的 leader，或者进入选举阶段等。
     * 同样 LeaderChangeContext 描述了停止 follow 的 leader 的信息，其中 status 描述了停止 follow 的原因。
     * */
    @Override
    public void onStopFollowing(LeaderChangeContext ctx) {
        log.info("Stop following, ctx={}", ctx);
    }

    /**
     * 当一个 raft follower 或者 candidate 节点开始 follow 一个 leader 的时候调用，
     * LeaderChangeContext 包含了 leader 的 PeerId/term/status 等上下文信息。
     * 并且当前 raft node 的 leaderId 属性会被设置为新的 leader 节点 PeerId。
     * */
    @Override
    public void onStartFollowing(LeaderChangeContext ctx) {
        log.info("Start following, ctx={}", ctx);
    }
}

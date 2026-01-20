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

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.Controller;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.impl.closure.ControllerClosure;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetSyncStateDataRequest;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 在 Controller 中，我们将 Raft 算法用于选举 Controller 中的 Active Controller，
 * 由它来负责处理数据面的选举、同步等任务，其余的 Controller 则只负责同步 Active Controller 的处理结果。
 * 这样的设计能够保证 Controller 本身也是高可用的，且保证了仅有一个 Controller 在处理 Broker 的选举事务。
 *
 * https://mp.weixin.qq.com/s/TT_NpT3xV9Yhmva73E85Hg
 *
 * Raft 像是小组作业，同学们（Broker）互相投票进行小组长的票选，而 3S 算法则由班主任（Controller）根据举手快慢直接任命。
 *
 * 这种设计的好处在哪里呢？Raft 算法的实现原理其实是“投票”，同学间彼此平等，靠投票结果“少数服从多数”。
 * 因此，对于一个有 2n+1 节点的集群来说，Raft 最多只能容忍n个节点失效，至少需要保证有 n+1 个节点是持续运行的。
 * 但是 3S 算法有一个选举中心，每次选举的 RPC 都向上发送，它不需要得到其它节点的认可便可选举出一个节点。
 * 因此对于之前提到的 2n+1 节点的集群来说，最多能容忍 2n 个节点的失效，即副本的数量不需要超过副本总数的一半，不需要满足 “多数派” 原则。
 * 通常，副本数大于等于 2 即可，如此，便在可靠性和吞吐量方面取得平衡。
 * */
public class JRaftController implements Controller {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final RaftGroupService raftGroupService;
    private Node node;
    private final JRaftControllerStateMachine stateMachine;
    private final ControllerConfig controllerConfig;
    // org.apache.rocketmq.controller.ControllerManager.onBrokerInactive
    private final List<BrokerLifecycleListener> brokerLifecycleListeners;
    private final Map<PeerId/* jRaft peerId */, String/* Controller RPC Server Addr */> peerIdToAddr;
    private final NettyRemotingServer remotingServer;

    public JRaftController(ControllerConfig controllerConfig,
        final ChannelEventListener channelEventListener) throws IOException {
        this.controllerConfig = controllerConfig;
        this.brokerLifecycleListeners = new ArrayList<>();

        final NodeOptions nodeOptions = new NodeOptions();
        // 一个 follower 当超过这个设定时间没有收到 leader 的消息后，变成 candidate 节点的时间。
        // leader 会在 electionTimeoutMs 时间内向 follower 发消息（心跳或者复制日志），如果没有收到，
        // follower 就需要进入 candidate状态，发起选举或者等待新的 leader 出现，默认1秒。
        // 如果节点是 leader，那么 raft group 在最多 election timeout 时间后开始选举，产生新的 leader。
        // 在产生新 leader 之前，写入服务终止，读服务继续提供，但是可能频繁遇到脏读。线性一致读也将无法服务。
        nodeOptions.setElectionTimeoutMs(controllerConfig.getJraftConfig().getjRaftElectionTimeoutMs());
        // 自动 Snapshot 间隔时间，默认一个小时
        nodeOptions.setSnapshotIntervalSecs(controllerConfig.getJraftConfig().getjRaftSnapshotIntervalSecs());
        // 本 raft 节点
        final PeerId serverId = new PeerId();// PeerId 表示一个 raft 参与节点
        if (!serverId.parse(controllerConfig.getJraftConfig().getjRaftServerId())) {
            throw new IllegalArgumentException("Fail to parse serverId:" + controllerConfig.getJraftConfig().getjRaftServerId());
        }
        // Configuration 表示一个 raft group 配置，也就是节点列表
        // 当节点是从一个空白状态启动（snapshot和log存储都为空），那么他会使用这个初始配置作为 raft group
        // 的配置启动，否则会从存储中加载已有配置。
        final Configuration initConf = new Configuration();
        if (!initConf.parse(controllerConfig.getJraftConfig().getjRaftInitConf())) {
            throw new IllegalArgumentException("Fail to parse initConf:" + controllerConfig.getJraftConfig().getjRaftInitConf());
        }
        // group 中的 raft 节点地址
        // 当节点是从一个空白状态启动（snapshot和log存储都为空），那么他会使用这个初始配置作为 raft group
        // 的配置启动，否则会从存储中加载已有配置。
        nodeOptions.setInitialConf(initConf);

        FileUtils.forceMkdir(new File(controllerConfig.getControllerStorePath()));
        // Log 存储，记录 raft 配置变更和用户提交任务的日志，将从 Leader 复制到其他节点上。LogStorage 是存储实现，
        // LogManager 负责对底层存储的调用，对调用做缓存、批量提交、必要的检查和优化
        // Raft 节点的日志存储路径，必须有
        nodeOptions.setLogUri(controllerConfig.getControllerStorePath() + File.separator + "log");
        // Meta 存储，元信息存储,记录 raft 实现的内部状态，比如当前 term,、投票给哪个节点等信息。
        // Raft 节点的元信息存储路径，必须有
        nodeOptions.setRaftMetaUri(controllerConfig.getControllerStorePath() + File.separator + "raft_meta");
        // Snapshot 存储,，用于存放用户的状态机 snapshot 及元信息，可选。 SnapshotStorage 用于 snapshot 存储实现，
        // SnapshotExecutor 用于 snapshot 实际存储、远程安装、复制的管理。
        // Raft 节点的 snapshot 存储路径，可选，不提供就关闭了 snapshot 功能。
        nodeOptions.setSnapshotUri(controllerConfig.getControllerStorePath() + File.separator + "snapshot");
        // 本机 raft 节点状态机
        // StateMachine： 用户核心逻辑的实现,核心是 onApply(Iterator) 方法，应用通过 Node#apply(task) 提交的日志到业务状态机。
        this.stateMachine = new JRaftControllerStateMachine(controllerConfig, new NodeId(controllerConfig.getJraftConfig().getjRaftGroupId(), serverId));
        //  当状态机所属的 raft 节点成为 leader 的时候被调用，成为 leader 当前的 term 通过参数传入。
        this.stateMachine.registerOnLeaderStart(this::onLeaderStart);
        // 当前状态机所属的 raft 节点失去 leader 资格时调用，status 字段描述了详细的原因，比如主动转移 leadership、重新发生选举等。
        this.stateMachine.registerOnLeaderStop(this::onLeaderStop);
        // FSMCaller： 封装对业务 StateMachine 的状态转换的调用以及日志的写入等，一个有限状态机的实现，做必要的检查、请求合并提交和并发处理等。
        // 最核心的，属于本 raft 节点的应用状态机实例。
        nodeOptions.setFsm(this.stateMachine);

        this.raftGroupService = new RaftGroupService(controllerConfig.getJraftConfig().getjRaftGroupId(), serverId, nodeOptions);

        this.peerIdToAddr = new HashMap<>();
        initPeerIdMap();

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // jRaftControllerRPCAddr(提供给 broker 与 controller 进行通信)
        nettyServerConfig.setListenPort(Integer.parseInt(this.peerIdToAddr.get(serverId).split(":")[1]));
        remotingServer = new NettyRemotingServer(nettyServerConfig, channelEventListener);
    }

    private void initPeerIdMap() {
        String[] peers = this.controllerConfig.getJraftConfig().getjRaftInitConf().split(",");
        String[] rpcAddrs = this.controllerConfig.getJraftConfig().getjRaftControllerRPCAddr().split(",");
        for (int i = 0; i < peers.length; i++) {
            PeerId peerId = new PeerId();
            if (!peerId.parse(peers[i])) {
                throw new IllegalArgumentException("Fail to parse peerId:" + peers[i]);
            }
            this.peerIdToAddr.put(peerId, rpcAddrs[i]);
        }
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.node = this.raftGroupService.start();
        log.info("Controller {} started.", node.getNodeId());
    }

    @Override
    public void shutdown() {
        this.stopScheduling();
        this.raftGroupService.shutdown();
        this.remotingServer.shutdown();
        log.info("Controller {} stopped.", node.getNodeId());
    }

    @Override
    public void startScheduling() {
    }

    @Override
    public void stopScheduling() {
    }

    @Override
    public boolean isLeaderState() {
        return node.isLeader();
    }

    private <T extends CommandCustomHeader> CompletableFuture<RemotingCommand> applyToJRaft(RemotingCommand request) {
        if (!isLeaderState()) {
            final RemotingCommand command = RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_NOT_LEADER, "The controller is not in leader state");
            final CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
            future.complete(command);
            log.warn("Apply to none leader controller, controller state is {}", node.getNodeState());
            return future;
        }
        // Closure done 任务的回调，在任务完成的时候通知此对象，无论成功还是失败。
        // 这个 closure 将在 StateMachine#onApply(iterator) 方法应用到状态机的时候，可以拿到并调用，一般用于客户端应答的返回。
        ControllerClosure closure = new ControllerClosure(request);
        Task task = closure.taskWithThisClosure();
        if (task != null) {
            node.apply(task);
            return closure.getFuture();
        } else {
            log.error("Apply task failed, task is null.");
            return CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, "Apply task failed, Please see the server log."));
        }
    }

    @Override
    public CompletableFuture<RemotingCommand> alterSyncStateSet(AlterSyncStateSetRequestHeader request,
        SyncStateSet syncStateSet) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, request);
        requestCommand.setBody(syncStateSet.encode());
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> electMaster(ElectMasterRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getNextBrokerId(GetNextBrokerIdRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> applyBrokerId(ApplyBrokerIdRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_APPLY_BROKER_ID, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> registerBroker(RegisterBrokerToControllerRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_REGISTER_BROKER, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getReplicaInfo(GetReplicaInfoRequestHeader request) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_REPLICA_INFO, request);
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> getSyncStateData(List<String> brokerNames) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, new GetSyncStateDataRequest());
        requestCommand.setBody(RemotingSerializable.encode(brokerNames));
        return applyToJRaft(requestCommand);
    }

    @Override
    public CompletableFuture<RemotingCommand> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CLEAN_BROKER_DATA, requestHeader);
        return applyToJRaft(requestCommand);
    }

    @Override
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {
        // org.apache.rocketmq.controller.ControllerManager.onBrokerInactive
        this.brokerLifecycleListeners.add(listener);
    }

    @Override
    public RemotingCommand getControllerMetadata() {
        List<PeerId> peers = node.getOptions().getInitialConf().getPeers();
        final StringBuilder sb = new StringBuilder();
        for (PeerId peer : peers) {
            sb.append(peerIdToAddr.get(peer)).append(";"); // 所有 controller 节点（端口是对 broker 提供的 rpc 端口）
        }
        return RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, new GetMetaDataResponseHeader(
            node.getGroupId(),
            node.getLeaderId() == null ? "" : node.getLeaderId().toString(),
            this.peerIdToAddr.get(node.getLeaderId()), // leader 对 broker 的 rpc 地址
            node.isLeader(),
            sb.toString()
        ));
    }

    @Override
    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void onLeaderStart(long term) {
        log.info("Controller start leadership, term: {}.", term);
    }

    public void onLeaderStop(Status status) {
        log.info("Controller {} stop leadership, status: {}.", node.getNodeId(), status);
        this.stopScheduling();
    }

    public CompletableFuture<RemotingCommand> getBrokerLiveInfo(GetBrokerLiveInfoRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_LIVE_INFO_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> onBrokerHeartBeat(RaftBrokerHeartBeatEventRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.RAFT_BROKER_HEART_BEAT_EVENT_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> onBrokerCloseChannel(BrokerCloseChannelRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.BROKER_CLOSE_CHANNEL_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }

    public CompletableFuture<RemotingCommand> checkNotActiveBroker(CheckNotActiveBrokerRequest requestHeader) {
        final RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CHECK_NOT_ACTIVE_BROKER_REQUEST, requestHeader);
        return applyToJRaft(requestCommand);
    }
}

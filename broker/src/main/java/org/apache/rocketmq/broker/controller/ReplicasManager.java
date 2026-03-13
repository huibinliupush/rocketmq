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

package org.apache.rocketmq.broker.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.autoswitch.BrokerMetadata;
import org.apache.rocketmq.store.ha.autoswitch.TempBrokerMetadata;

import static org.apache.rocketmq.remoting.protocol.ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST;

/**
 * The manager of broker replicas, including: 0.regularly syncing controller metadata, change controller leader address,
 * both master and slave will start this timed task. 1.regularly syncing metadata from controllers, and changing broker
 * roles and master if needed, both master and slave will start this timed task. 2.regularly expanding and Shrinking
 * syncStateSet, only master will start this timed task.
 */
public class ReplicasManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final int RETRY_INTERVAL_SECOND = 5;
    // 3线程
    private final ScheduledExecutorService scheduledService;
    // 4线程
    private final ExecutorService executorService;
    // 4线程，max 10 线程, 32队列容量
    private final ExecutorService scanExecutor;
    private final BrokerController brokerController;
    private final AutoSwitchHAService haService;
    private final BrokerConfig brokerConfig;
    // BrokerIP1 : nettyServerListenPort
    private final String brokerAddress;
    private final BrokerOuterAPI brokerOuterAPI;
    // 通过 dns 或者配置获取, 每 120s 更新
    // see : org.apache.rocketmq.broker.controller.ReplicasManager.start
    private List<String> controllerAddresses;
    // 可用的 controller 地址
    // org.apache.rocketmq.broker.controller.ReplicasManager.scanAvailableControllerAddresses
    // 每 3s 扫描更新
    // see : org.apache.rocketmq.broker.controller.ReplicasManager.start
    private final ConcurrentMap<String, Boolean> availableControllerAddresses;
    // active controller 地址
    // see : org.apache.rocketmq.broker.controller.ReplicasManager.updateControllerMetadata
    private volatile String controllerLeaderAddress = "";
    private volatile State state = State.INITIAL;

    private volatile RegisterState registerState = RegisterState.INITIAL;

    private ScheduledFuture<?> checkSyncStateSetTaskFuture;
    private ScheduledFuture<?> slaveSyncFuture;
    // controller 为 broker 分配的 id
    // 对于 slave 来说，brokerId 就是这里的
    // 对于 master 来说， 这里仍然是 controller 分配的 id
    // 但是这里 org.apache.rocketmq.common.BrokerIdentity.brokerId 会被设置为 0
    private Long brokerControllerId;

    private Long masterBrokerId;
    // user.home/store/brokerIdentity
    private BrokerMetadata brokerMetadata;
    // user.home/store/brokerIdentity-temp
    private TempBrokerMetadata tempBrokerMetadata;

    private Set<Long> syncStateSet;
    // syncStateSet 每改变一次， syncStateSetEpoch + 1
    private int syncStateSetEpoch = 0;
    private String masterAddress = "";
    private int masterEpoch = 0;
    private long lastSyncTimeMs = System.currentTimeMillis();
    private Random random = new Random();

    public ReplicasManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
        this.scheduledService = ThreadUtils.newScheduledThreadPool(3, new ThreadFactoryImpl("ReplicasManager_ScheduledService_", brokerController.getBrokerIdentity()));
        this.executorService = ThreadUtils.newThreadPoolExecutor(3, new ThreadFactoryImpl("ReplicasManager_ExecutorService_", brokerController.getBrokerIdentity()));
        this.scanExecutor = ThreadUtils.newThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("ReplicasManager_scan_thread_", brokerController.getBrokerIdentity()));
        this.haService = (AutoSwitchHAService) brokerController.getMessageStore().getHaService();
        this.brokerConfig = brokerController.getBrokerConfig();
        this.availableControllerAddresses = new ConcurrentHashMap<>();
        this.syncStateSet = new HashSet<>();
        // BrokerIP1 : nettyServerListenPort
        this.brokerAddress = brokerController.getBrokerAddr();
        // user.home/store/brokerIdentity
        this.brokerMetadata = new BrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity());
        // user.home/store/brokerIdentity-temp
        this.tempBrokerMetadata = new TempBrokerMetadata(this.brokerController.getMessageStoreConfig().getStorePathBrokerIdentity() + "-temp");
    }

    enum State {
        INITIAL,
        FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE, // 成功获取到 active controller 地址之后的状态
        REGISTER_TO_CONTROLLER_DONE, // 向 controller 获取 brokerId ,以及初始化 controller 中的 副本信息集合(第一次注册的话，还未产生 master)
        RUNNING, // master 选举出来之后，状态才会变为 running
        SHUTDOWN,
    }

    enum RegisterState {
        INITIAL,
        CREATE_TEMP_METADATA_FILE_DONE, // 在向 controller 获取到 brokerId 之后，并且将 broker 相关信息保存在 tempBrokerMetadata 文件中
        CREATE_METADATA_FILE_DONE, // apply brokerId 到 controller 之后 ，将正式的 broker 元数据更新到 brokerMetadata，并删除 temp
        REGISTERED
    }

    public void start() {
        this.state = State.INITIAL;
        // 获取 controller 地址 by dns or config
        updateControllerAddr();
        // 调用 brokerOuterAPI.checkAddressReachable 挨个检查 controller 地址是否可用
        // 将健康的 controller 连接存放在 availableControllerAddresses
        scanAvailableControllerAddresses();
        // 120s
        this.scheduledService.scheduleAtFixedRate(this::updateControllerAddr, 2 * 60 * 1000, 2 * 60 * 1000, TimeUnit.MILLISECONDS);
        // 3s
        this.scheduledService.scheduleAtFixedRate(this::scanAvailableControllerAddresses, 3 * 1000, 3 * 1000, TimeUnit.MILLISECONDS);
        // 启动 broker 与 controller 的交互流程
        // 获取 brokerId , role ， 根据 role 变换 master or slave 并开始各自的 ha 逻辑
        if (!startBasicService()) {
            LOGGER.error("Failed to start replicasManager");
            // 3线程
            this.executorService.submit(() -> {
                int retryTimes = 0;
                do {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECOND);// 5s
                    } catch (InterruptedException ignored) {

                    }
                    retryTimes++;
                    LOGGER.warn("Failed to start replicasManager, retry times:{}, current state:{}, try it again", retryTimes, this.state);
                }
                while (!startBasicService());

                LOGGER.info("Start replicasManager success, retry times:{}", retryTimes);
            });
        }
    }

    private boolean startBasicService() {
        if (this.state == State.SHUTDOWN)
            return false;
        if (this.state == State.INITIAL) {
            // 获取 active controller 地址
            if (schedulingSyncControllerMetadata()) {
                // 获取成功
                this.state = State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE;
                LOGGER.info("First time sync controller metadata success, change state to: {}", this.state);
            } else {
                // 获取失败
                return false;
            }
        }

        if (this.state == State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE) {
            for (int retryTimes = 0; retryTimes < 5; retryTimes++) {
                // 向 controller 获取 brokerId ,以及初始化 controller 中的 副本信息集合(第一次注册时，这里不会产生 master)
                if (register()) {
                    this.state = State.REGISTER_TO_CONTROLLER_DONE;
                    LOGGER.info("First time register broker success, change state to: {}", this.state);
                    break;
                }

                // Try to avoid registration concurrency conflicts in random sleep
                try {
                    Thread.sleep(random.nextInt(1000));
                } catch (Exception ignore) {

                }
            }
            // register 5 times but still unsuccessful
            if (this.state != State.REGISTER_TO_CONTROLLER_DONE) {
                LOGGER.error("Register to broker failed 5 times");
                // 隔 5s 重试
                return false;
            }
        }

        if (this.state == State.REGISTER_TO_CONTROLLER_DONE) {
            // The scheduled task for heartbeat sending is not starting now, so we should manually send heartbeat request
            // 向所有 controller 发送第一次心跳
            this.sendHeartbeatToController();
            // register 到 controller 之后，这里才开始选举 master broker
            // brokerElect 之后各个 broker 的 role 就已经确定了，并开始各自的 ha 处理逻辑
            if (this.masterBrokerId != null || brokerElect()) {
                LOGGER.info("Master in this broker set is elected, masterBrokerId: {}, masterBrokerAddr: {}", this.masterBrokerId, this.masterAddress);
                this.state = State.RUNNING;
                setFenced(false);
                LOGGER.info("All register process has been done, change state to: {}", this.state);
            } else {
                return false;
            }
        }
        // 每 5s 向 controller 同步 broker metadata
        // metadata 数据包括 SyncStateSet ， master 信息， epoch
        // 如果 controller 触发选主，那么 broker 就是通过这里的定时任务来感知的
        schedulingSyncBrokerMetadata();

        // Register syncStateSet changed listener.
        // 如果 master 检测到 syncStateSet 需要发生改变，那么就会通过这个回调来向 controller 发起变更 syncStateSet
        this.haService.registerSyncStateSetChangedListener(this::doReportSyncStateSetChanged);
        return true;
    }

    public void shutdown() {
        this.state = State.SHUTDOWN;
        this.registerState = RegisterState.INITIAL;
        this.executorService.shutdownNow();
        this.scheduledService.shutdownNow();
        this.scanExecutor.shutdownNow();
    }

    public synchronized void changeBrokerRole(final Long newMasterBrokerId, final String newMasterAddress,
        final Integer newMasterEpoch,
        final Integer syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        if (newMasterBrokerId != null && newMasterEpoch > this.masterEpoch) {
            if (newMasterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(newMasterEpoch, syncStateSetEpoch, syncStateSet);
            } else {
                changeToSlave(newMasterAddress, newMasterEpoch, newMasterBrokerId);
            }
        }
    }

    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch, final Set<Long> syncStateSet) throws Exception {
        synchronized (this) {
            // controller 第一次选主之后会将 newMasterEpoch 变为 1 ，也就是 epoch 从 1 开始
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to master, brokerName:{}, replicas:{}, new Epoch:{}", this.brokerConfig.getBrokerName(), this.brokerAddress, newMasterEpoch);
                this.masterEpoch = newMasterEpoch;
                // 该节点之前就是 master , 只是更改 epoch 相关 ha 信息就可以了，因为相关 commitlog ,consume queue 本身就是完整的，不必截断
                if (this.masterBrokerId != null && this.masterBrokerId.equals(this.brokerControllerId) && this.brokerController.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
                    // Change SyncStateSet
                    final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                    changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);
                    // if master doesn't change
                    // 新增 epoch, startOffset 为当前 commitlog 的最大 offset, endOffset 为无限大
                    this.haService.changeToMasterWhenLastRoleIsMaster(newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    // 重新检查SyncStateSet中的slave是否需要剔除
                    // 1.curTime - HaConnection.lastCaughtUpTime) > haMaxTimeSlaveNotCatchup(15s)
                    // 2， slave 不活跃不在connectionCaughtUpTimeTable中
                    // 如果SyncStateSet发生变动则向controller报告
                    this.executorService.submit(this::checkSyncStateSetAndDoReport);
                    // 当 broker 的角色变更之后需要向 nameserver 重新注册
                    registerBrokerWhenRoleChange();
                    return;
                }
                // 如果 broker 之前是 slave ，那么其 commitlog ，consume queue 可能是不完整的，这样就需要截断无效的日志
                // Change SyncStateSet
                final HashSet<Long> newSyncStateSet = new HashSet<>(syncStateSet);
                changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);

                // Handle the slave synchronise
                // SlaveSynchronize 模块的 master 地址设为 null
                handleSlaveSynchronize(BrokerRole.SYNC_MASTER);

                // Notify ha service, change to master
                // 截断 commitlog ，consume queue， 修正相关 offset , 以及 epoch cache
                this.haService.changeToMaster(newMasterEpoch);

                this.brokerController.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
                // 这里只会设置 SYNC_MASTER ， 通过配置需要 ack 的副本数来设置 ASYNC_MASTER
                // ackNum = 0 , 即为 ASYNC_MASTER ， acNum != 0 则为 SYNC_MASTER
                // see : GroupTransferService
                // 应答的 slave 不需要是 syncStateSet 中的 slave，ackNum = -1 则是需要syncStateSet 中的所有 slave 应答
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);
                // 启动 scheduleMessageService， 调度定时消息，将到期的定时消息 reput 到 commitlog
                // 以及事务消息
                this.brokerController.changeSpecialServiceStatus(true);

                // Change record
                this.masterAddress = this.brokerAddress;
                this.masterBrokerId = this.brokerControllerId;
                // 每 5s 执行 checkSyncStateSetAndDoReport
                // 检查各个 ha conntion , 确认是否改变 SyncStateSet 并向 controller 汇报
                schedulingCheckSyncStateSet();

                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                this.executorService.submit(this::checkSyncStateSetAndDoReport);
                registerBrokerWhenRoleChange();
            }
        }
    }

    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, Long newMasterBrokerId) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to slave, brokerName={}, brokerId={}, newMasterBrokerId={}, newMasterAddress={}, newMasterEpoch={}",
                    this.brokerConfig.getBrokerName(), this.brokerControllerId, newMasterBrokerId, newMasterAddress, newMasterEpoch);

                this.masterEpoch = newMasterEpoch;
                // master 没有变，还是原来的 master, 本 broker 还是原来的 slave
                if (newMasterBrokerId.equals(this.masterBrokerId)) {
                    // if master doesn't change
                    this.haService.changeToSlaveWhenMasterNotChange(newMasterAddress, newMasterEpoch);
                    this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                    registerBrokerWhenRoleChange();
                    return;
                }
                // 停止 checkSyncStateSetTask 如果有的话，因为只有 master 才负责变更 SyncStateSet
                // Stop checking syncStateSet because only master is able to check
                stopCheckSyncStateSet();

                // Change config(compatibility problem)
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
                // 停止 scheduleMessageService， 调度定时消息，将到期的定时消息 reput 到 commitlog（停止）
                // 停止事务消息 check
                this.brokerController.changeSpecialServiceStatus(false);
                // The brokerId in brokerConfig just means its role(master[0] or slave[>=1])
                this.brokerConfig.setBrokerId(brokerControllerId);

                // Change record
                this.masterAddress = newMasterAddress;
                this.masterBrokerId = newMasterBrokerId; // controller 里的 masterBrokerId

                // Handle the slave synchronise
                // 开启 slaveSyncFuture，向 master 同步消息的元信息：topicConfig,TopicQueueMapping,ConsumerOffset,SubscriptionGroupConfig（每 10s 向 master 同步一次）
                // 每 3 秒同步一次 TimerCheckpoint
                // TimerCheckpoint 用于持久化时间轮的时间指针，重启之后，时间轮可以继续从上次的时间点继续转动
                handleSlaveSynchronize(BrokerRole.SLAVE);

                // Notify ha service, change to slave
                // 创建 haClient , 相关的 ha 阶段 handshake , transfer 都是在这里启动的
                this.haService.changeToSlave(newMasterAddress, newMasterEpoch, brokerControllerId);

                this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(newMasterEpoch);
                registerBrokerWhenRoleChange();
            }
        }
    }

    public void registerBrokerWhenRoleChange() {

        this.executorService.submit(() -> {
            // Register broker to name-srv
            try {
                this.brokerController.registerBrokerAll(true, false, this.brokerController.getBrokerConfig().isForceRegister());
            } catch (final Throwable e) {
                LOGGER.error("Error happen when register broker to name-srv, Failed to change broker to {}", this.brokerController.getMessageStoreConfig().getBrokerRole(), e);
                return;
            }
            LOGGER.info("Change broker [id:{}][address:{}] to {}, newMasterBrokerId:{}, newMasterAddress:{}, newMasterEpoch:{}, syncStateSetEpoch:{}",
                this.brokerControllerId, this.brokerAddress, this.brokerController.getMessageStoreConfig().getBrokerRole(), this.masterBrokerId, this.masterAddress, this.masterEpoch, this.syncStateSetEpoch);
        });

    }

    private void changeSyncStateSet(final Set<Long> newSyncStateSet, final int newSyncStateSetEpoch) {
        synchronized (this) {
            if (newSyncStateSetEpoch > this.syncStateSetEpoch) {
                LOGGER.info("SyncStateSet changed from {} to {}", this.syncStateSet, newSyncStateSet);
                this.syncStateSetEpoch = newSyncStateSetEpoch;
                this.syncStateSet = new HashSet<>(newSyncStateSet);
                this.haService.setSyncStateSet(newSyncStateSet);
            }
        }
    }

    private void handleSlaveSynchronize(final BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (this.slaveSyncFuture != null) {
                // 因为 master 变了，需要重新向新的 master 同步
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(this.masterAddress);
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                        // 每 10 秒向 master 同步消息的元信息：topicConfig,TopicQueueMapping,ConsumerOffset,SubscriptionGroupConfig
                        brokerController.getSlaveSynchronize().syncAll();
                        lastSyncTimeMs = System.currentTimeMillis();
                    }
                    //timer checkpoint, latency-sensitive, so sync it more frequently
                    // 每 3 秒同步一次 TimerCheckpoint ，因为定时任务延时敏感，所以需要同步的频率高一些
                    // TimerCheckpoint 用于持久化时间轮的时间指针，重启之后，时间轮可以继续从上次的时间点继续转动
                    brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                } catch (final Throwable e) {
                    LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);

        } else {
            // broker 作为 slave 的时候才会有，现在变为 master 了就取消掉
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            // 自己就是 master , 所以 SlaveSynchronize 模块的 master 地址设为 null
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    private boolean brokerElect() {
        // Broker try to elect itself as a master in broker set.
        try {
            Pair<ElectMasterResponseHeader, Set<Long>> tryElectResponsePair = this.brokerOuterAPI.brokerElect(this.controllerLeaderAddress, this.brokerConfig.getBrokerClusterName(),
                this.brokerConfig.getBrokerName(), this.brokerControllerId);
            ElectMasterResponseHeader tryElectResponse = tryElectResponsePair.getObject1();
            // 每次重新选举之后，SyncStateSet 都只有一个元素，那就是新的 master
            Set<Long> syncStateSet = tryElectResponsePair.getObject2();
            final String masterAddress = tryElectResponse.getMasterAddress();
            final Long masterBrokerId = tryElectResponse.getMasterBrokerId();
            if (StringUtils.isEmpty(masterAddress) || masterBrokerId == null) {
                LOGGER.warn("Now no master in broker set");
                return false;
            }

            if (masterBrokerId.equals(this.brokerControllerId)) {
                changeToMaster(tryElectResponse.getMasterEpoch(), tryElectResponse.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, tryElectResponse.getMasterEpoch(), tryElectResponse.getMasterBrokerId());
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to try elect", e);
            return false;
        }
    }

    public void sendHeartbeatToController() {
        final List<String> controllerAddresses = this.getAvailableControllerAddresses();
        for (String controllerAddress : controllerAddresses) {
            if (StringUtils.isNotEmpty(controllerAddress)) {
                this.brokerOuterAPI.sendHeartbeatToController(
                    controllerAddress,
                    this.brokerConfig.getBrokerClusterName(),
                    this.brokerAddress,
                    this.brokerConfig.getBrokerName(),
                    this.brokerControllerId, // controller 为 broker 分配的 brokerId
                    this.brokerConfig.getSendHeartbeatTimeoutMillis(), // 1s
                    this.brokerConfig.isInBrokerContainer(), this.getLastEpoch(),
                    this.brokerController.getMessageStore().getMaxPhyOffset(),
                    this.brokerController.getMessageStore().getConfirmOffset(),
                    this.brokerConfig.getControllerHeartBeatTimeoutMills(), // 10s
                    this.brokerConfig.getBrokerElectionPriority() // 值越小，当选 master 的概率越大
                );
            }
        }
    }

    /**
     * Register broker to controller, and persist the metadata to file
     *
     * @return whether registering process succeeded
     */
    private boolean register() {
        try {
            // 1. confirm now registering state
            // 从 brokerMetadata 文件中初始化 registerState ， brokerId(刚启动时还是空的，registerState 仍为 INITIAL)
            confirmNowRegisteringState();
            LOGGER.info("Confirm now register state: {}", this.registerState);
            // 2. check metadata/tempMetadata if valid
            // 查看 brokerMetadata 文件中的数据是否和 brokerConfig 中是一致的
            if (!checkMetadataValid()) {
                LOGGER.error("Check and find that metadata/tempMetadata invalid, you can modify the broker config to make them valid");
                return false;
            }
            // 2. get next assigning brokerId, and create temp metadata file
            if (this.registerState == RegisterState.INITIAL) {
                // 获取 BrokerId
                Long nextBrokerId = getNextBrokerId();
                // 将 broker 的相关信息保存在 TempMetadataFile 中
                if (nextBrokerId == null || !createTempMetadataFile(nextBrokerId)) {
                    LOGGER.error("Failed to create temp metadata file, nextBrokerId: {}", nextBrokerId);
                    return false;
                }
                this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
                LOGGER.info("Register state change to {}, temp metadata: {}", this.registerState, this.tempBrokerMetadata);
            }
            // 3. apply brokerId to controller, and create metadata file
            if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
                if (!applyBrokerId()) {
                    // apply broker id failed, means that this brokerId has been used
                    // delete temp metadata file
                    this.tempBrokerMetadata.clear();
                    // back to the first step
                    this.registerState = RegisterState.INITIAL;
                    LOGGER.info("Register state change to: {}", this.registerState);
                    return false;
                }
                // apply brokerId 到 controller 之后 ，将正式的 broker 元数据更新到 brokerMetadata，并删除 temp
                if (!createMetadataFileAndDeleteTemp()) {
                    LOGGER.error("Failed to create metadata file and delete temp metadata file, temp metadata: {}", this.tempBrokerMetadata);
                    return false;
                }
                this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
                LOGGER.info("Register state change to: {}, metadata: {}", this.registerState, this.brokerMetadata);
            }
            // 4. register
            if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
                if (!registerBrokerToController()) { // broker 第一次启动注册时候这里还未产生 master
                    LOGGER.error("Failed to register broker to controller");
                    return false;
                }
                this.registerState = RegisterState.REGISTERED;
                LOGGER.info("Register state change to: {}, masterBrokerId: {}, masterBrokerAddr: {}", this.registerState, this.masterBrokerId, this.masterAddress);
            }
            return true;
        } catch (final Exception e) {
            LOGGER.error("Failed to register broker to controller", e);
            return false;
        }
    }

    /**
     * Send GetNextBrokerRequest to controller for getting next assigning brokerId in this broker-set
     *
     * @return next brokerId in this broker-set
     */
    private Long getNextBrokerId() {
        try {
            GetNextBrokerIdResponseHeader nextBrokerIdResp = this.brokerOuterAPI.getNextBrokerId(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.controllerLeaderAddress);
            return nextBrokerIdResp.getNextBrokerId();
        } catch (Exception e) {
            LOGGER.error("fail to get next broker id from controller", e);
            return null;
        }
    }

    /**
     * Create temp metadata file in local file system, records the brokerId and registerCheckCode
     *
     * @param brokerId the brokerId that is expected to be assigned
     * @return whether the temp meta file is created successfully
     */

    private boolean createTempMetadataFile(Long brokerId) {
        // generate register check code, format like that: $ipAddress;$timestamp
        String registerCheckCode = this.brokerAddress + ";" + System.currentTimeMillis();
        try {
            this.tempBrokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerId, registerCheckCode);
            return true;
        } catch (Exception e) {
            LOGGER.error("update and persist temp broker metadata file failed", e);
            this.tempBrokerMetadata.clear();
            return false;
        }
    }

    /**
     * Send applyBrokerId request to controller
     *
     * @return whether controller has assigned this brokerId for this broker
     */
    private boolean applyBrokerId() {
        try {
            ApplyBrokerIdResponseHeader response = this.brokerOuterAPI.applyBrokerId(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                tempBrokerMetadata.getBrokerId(), tempBrokerMetadata.getRegisterCheckCode(), this.controllerLeaderAddress);
            return true;

        } catch (Exception e) {
            LOGGER.error("fail to apply broker id: {}", tempBrokerMetadata.getBrokerId(), e);
            return false;
        }
    }

    /**
     * Create metadata file and delete temp metadata file
     *
     * @return whether process success
     */
    private boolean createMetadataFileAndDeleteTemp() {
        // create metadata file and delete temp metadata file
        try {
            this.brokerMetadata.updateAndPersist(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), tempBrokerMetadata.getBrokerId());
            this.tempBrokerMetadata.clear();
            this.brokerControllerId = this.brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to create metadata file", e);
            this.brokerMetadata.clear();
            return false;
        }
    }

    /**
     * Send registerBrokerToController request to inform controller that now broker has been registered successfully and
     * controller should update broker ipAddress if changed
     *
     * @return whether request success
     */
    private boolean registerBrokerToController() {
        try {
            Pair<RegisterBrokerToControllerResponseHeader, Set<Long>> responsePair = this.brokerOuterAPI.registerBrokerToController(brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(), brokerControllerId, brokerAddress, controllerLeaderAddress);
            if (responsePair == null)
                return false;
            RegisterBrokerToControllerResponseHeader response = responsePair.getObject1();
            Set<Long> syncStateSet = responsePair.getObject2();
            final Long masterBrokerId = response.getMasterBrokerId();
            final String masterAddress = response.getMasterAddress();
            if (masterBrokerId == null) { // controller 中副本集还没有 master
                // 第一次启动注册，这里还没有产生 master
                return true;
            }
            // 用于向一个现有的副本集中添加 broker,broker 注册之后会直接变为 slave
            if (this.brokerControllerId.equals(masterBrokerId)) {
                changeToMaster(response.getMasterEpoch(), response.getSyncStateSetEpoch(), syncStateSet);
            } else {
                changeToSlave(masterAddress, response.getMasterEpoch(), masterBrokerId);
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("fail to send registerBrokerToController request to controller", e);
            return false;
        }
    }

    /**
     * Confirm the registering state now
     */
    private void confirmNowRegisteringState() {
        // 1. check if metadata exist
        try {
            // 从 user.home/store/brokerIdentity 文件中初始化 brokerMetadata
            this.brokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read metadata file failed", e);
        }
        // 由于刚开始启动 user.home/store/brokerIdentity 文件还是空的，所以 brokerMetadata loaded 返回 false
        if (this.brokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_METADATA_FILE_DONE;
            this.brokerControllerId = brokerMetadata.getBrokerId();
            this.haService.setLocalBrokerId(this.brokerControllerId);
            return;
        }
        // 2. check if temp metadata exist
        try {
            // user.home/store/brokerIdentity-temp 文件中初始化 brokerMetadata
            this.tempBrokerMetadata.readFromFile();
        } catch (Exception e) {
            LOGGER.error("Read temp metadata file failed", e);
        }
        // 由于刚开始启动 user.home/store/brokerIdentity-temp  文件还是空的，所以 tempBrokerMetadata loaded 返回 false
        if (this.tempBrokerMetadata.isLoaded()) {
            this.registerState = RegisterState.CREATE_TEMP_METADATA_FILE_DONE;
        }
    }

    private boolean checkMetadataValid() {
        if (this.registerState == RegisterState.CREATE_TEMP_METADATA_FILE_DONE) {
            if (this.tempBrokerMetadata.getClusterName() == null || !this.tempBrokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker temp metadata is different from the clusterName: {} in broker config",
                    this.tempBrokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.tempBrokerMetadata.getBrokerName() == null || !this.tempBrokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker temp metadata is different from the brokerName: {} in broker config",
                    this.tempBrokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        if (this.registerState == RegisterState.CREATE_METADATA_FILE_DONE) {
            if (this.brokerMetadata.getClusterName() == null || !this.brokerMetadata.getClusterName().equals(this.brokerConfig.getBrokerClusterName())) {
                LOGGER.error("The clusterName: {} in broker metadata is different from the clusterName: {} in broker config",
                    this.brokerMetadata.getClusterName(), this.brokerConfig.getBrokerClusterName());
                return false;
            }
            if (this.brokerMetadata.getBrokerName() == null || !this.brokerMetadata.getBrokerName().equals(this.brokerConfig.getBrokerName())) {
                LOGGER.error("The brokerName: {} in broker metadata is different from the brokerName: {} in broker config",
                    this.brokerMetadata.getBrokerName(), this.brokerConfig.getBrokerName());
                return false;
            }
        }
        return true;
    }

    /**
     * Scheduling sync broker metadata form controller.
     * 每 5s 同步
     */
    private void schedulingSyncBrokerMetadata() {
        this.scheduledService.scheduleAtFixedRate(() -> {
            try {
                final Pair<GetReplicaInfoResponseHeader, SyncStateSet> result = this.brokerOuterAPI.getReplicaInfo(this.controllerLeaderAddress, this.brokerConfig.getBrokerName());
                final GetReplicaInfoResponseHeader info = result.getObject1();
                final SyncStateSet syncStateSet = result.getObject2();
                final String newMasterAddress = info.getMasterAddress();
                final int newMasterEpoch = info.getMasterEpoch();
                final Long masterBrokerId = info.getMasterBrokerId();
                synchronized (this) {
                    // Check if master changed
                    if (newMasterEpoch > this.masterEpoch) { // 产生新的选举
                        if (StringUtils.isNoneEmpty(newMasterAddress) && masterBrokerId != null) {
                            if (masterBrokerId.equals(this.brokerControllerId)) {
                                // If this broker is now the master
                                changeToMaster(newMasterEpoch, syncStateSet.getSyncStateSetEpoch(), syncStateSet.getSyncStateSet());
                            } else {
                                // If this broker is now the slave, and master has been changed
                                changeToSlave(newMasterAddress, newMasterEpoch, masterBrokerId);
                            }
                        } else {
                            // In this case, the master in controller is null, try elect in controller, this will trigger the electMasterEvent in controller.
                            brokerElect();
                        }
                    } else if (newMasterEpoch == this.masterEpoch) {
                        // Check if SyncStateSet changed
                        // 如果没有产生新的选举，那么就更新 SyncStateSet
                        if (isMasterState()) {
                            changeSyncStateSet(syncStateSet.getSyncStateSet(), syncStateSet.getSyncStateSetEpoch());
                        }
                    }
                }
            } catch (final MQBrokerException exception) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), exception);
                if (exception.getResponseCode() == CONTROLLER_BROKER_METADATA_NOT_EXIST) {
                    try {
                        // 获取 master 信息以及副本组的 SyncStateSet，从而决定 changeToMaster or changeToSlave
                        registerBrokerToController();
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ignore) {

                    }
                }
            } catch (final Exception e) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), e);
            }
        }, 3 * 1000, this.brokerConfig.getSyncBrokerMetadataPeriod(), TimeUnit.MILLISECONDS);
    }

    /**
     * Scheduling sync controller metadata.
     */
    private boolean schedulingSyncControllerMetadata() {
        // Get controller metadata first.
        int tryTimes = 0;
        while (tryTimes < 3) {
            // true 表示选主成功 ， false 表示选主还未完成 （选主指的是 controller 选主）
            boolean flag = updateControllerMetadata();
            if (flag) {
                // 每 10s 轮询 controller 元数据
                this.scheduledService.scheduleAtFixedRate(this::updateControllerMetadata, 1000 * 3, this.brokerConfig.getSyncControllerMetadataPeriod(), TimeUnit.MILLISECONDS);
                return true;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignore) {

            }
            tryTimes++;
        }
        LOGGER.error("Failed to init controller metadata, maybe the controllers in {} is not available", this.controllerAddresses);
        return false;
    }

    /**
     * Update controller leader address by rpc.
     */
    private boolean updateControllerMetadata() {
        for (String address : this.availableControllerAddresses.keySet()) {
            try {
                // see : org.apache.rocketmq.controller.processor.ControllerRequestProcessor.processRequest
                // 向 controller 获取元数据（主要是获取 active controller 地址）
                final GetMetaDataResponseHeader responseHeader = this.brokerOuterAPI.getControllerMetaData(address);
                // 如果此时 controller 还未完成选主那么 ControllerLeaderAddress 就是空(返回值 flag 为 false)
                // broker 会每隔 1s 来轮询选主结果
                // org.apache.rocketmq.broker.controller.ReplicasManager.schedulingSyncControllerMetadata
                if (responseHeader != null && StringUtils.isNoneEmpty(responseHeader.getControllerLeaderAddress())) {
                    this.controllerLeaderAddress = responseHeader.getControllerLeaderAddress();
                    LOGGER.info("Update controller leader address to {}", this.controllerLeaderAddress);
                    return true;
                }
            } catch (final Exception e) {
                LOGGER.error("Failed to update controller metadata", e);
            }
        }
        return false;
    }

    /**
     * Scheduling check syncStateSet.
     */
    private void schedulingCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
        this.checkSyncStateSetTaskFuture = this.scheduledService.scheduleAtFixedRate(this::checkSyncStateSetAndDoReport, 3 * 1000,
            this.brokerConfig.getCheckSyncStateSetPeriod(), TimeUnit.MILLISECONDS);
    }

    private void checkSyncStateSetAndDoReport() {
        try {
            final Set<Long> newSyncStateSet = this.haService.maybeShrinkSyncStateSet();
            newSyncStateSet.add(this.brokerControllerId);
            synchronized (this) {
                if (this.syncStateSet != null) {
                    // Check if syncStateSet changed
                    if (this.syncStateSet.size() == newSyncStateSet.size() && this.syncStateSet.containsAll(newSyncStateSet)) {
                        return;
                    }
                }
            }
            doReportSyncStateSetChanged(newSyncStateSet);
        } catch (Exception e) {
            LOGGER.error("Check syncStateSet error", e);
        }
    }

    private void doReportSyncStateSetChanged(Set<Long> newSyncStateSet) {
        try {
            final SyncStateSet result = this.brokerOuterAPI.alterSyncStateSet(this.controllerLeaderAddress, this.brokerConfig.getBrokerName(), this.brokerControllerId, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
            if (result != null) {
                changeSyncStateSet(result.getSyncStateSet(), result.getSyncStateSetEpoch());
            }
        } catch (final Exception e) {
            LOGGER.error("Error happen when change SyncStateSet, broker:{}, masterAddress:{}, masterEpoch:{}, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}",
                this.brokerConfig.getBrokerName(), this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
        }
    }

    private void stopCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
    }

    private void scanAvailableControllerAddresses() {
        if (controllerAddresses == null) {
            LOGGER.warn("scanAvailableControllerAddresses addresses of controller is null!");
            return;
        }

        for (String address : availableControllerAddresses.keySet()) {
            // controllerAddresses 每隔 120s 会更新
            if (!controllerAddresses.contains(address)) {
                LOGGER.warn("scanAvailableControllerAddresses remove invalid address {}", address);
                availableControllerAddresses.remove(address);
            }
        }

        for (String address : controllerAddresses) {
            // 4线程，max 10 线程, 32队列容量
            scanExecutor.submit(() -> {
                if (brokerOuterAPI.checkAddressReachable(address)) {
                    availableControllerAddresses.putIfAbsent(address, true);
                } else {
                    Boolean value = availableControllerAddresses.remove(address);
                    if (value != null) {
                        LOGGER.warn("scanAvailableControllerAddresses remove unconnected address {}", address);
                    }
                }
            });
        }
    }

    private void updateControllerAddr() {
        if (brokerConfig.isFetchControllerAddrByDnsLookup()) {
            List<String> adders = brokerOuterAPI.dnsLookupAddressByDomain(this.brokerConfig.getControllerAddr());
            if (CollectionUtils.isNotEmpty(adders)) {
                this.controllerAddresses = adders;
            }
        } else {
            final String controllerPaths = this.brokerConfig.getControllerAddr();
            final String[] controllers = controllerPaths.split(";");
            assert controllers.length > 0;
            this.controllerAddresses = Arrays.asList(controllers);
        }
    }

    public int getLastEpoch() {
        return this.haService.getLastEpoch();
    }

    public BrokerRole getBrokerRole() {
        return this.brokerController.getMessageStoreConfig().getBrokerRole();
    }

    public boolean isMasterState() {
        return getBrokerRole() == BrokerRole.SYNC_MASTER;
    }

    public SyncStateSet getSyncStateSet() {
        return new SyncStateSet(this.syncStateSet, this.syncStateSetEpoch);
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public List<String> getControllerAddresses() {
        return controllerAddresses;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.haService.getEpochEntries();
    }

    public List<String> getAvailableControllerAddresses() {
        return new ArrayList<>(availableControllerAddresses.keySet());
    }

    public Long getBrokerControllerId() {
        return brokerControllerId;
    }

    public RegisterState getRegisterState() {
        return registerState;
    }

    public State getState() {
        return state;
    }

    public BrokerMetadata getBrokerMetadata() {
        return brokerMetadata;
    }

    public TempBrokerMetadata getTempBrokerMetadata() {
        return tempBrokerMetadata;
    }

    public void setFenced(boolean fenced) {
        // 在 broker 启动的时候会将 fenced 设置为 true ，设置一道屏障，使得 broker 处于孤立状态，不向 nameserver 注册
        // see : org.apache.rocketmq.broker.BrokerController.recoverAndInitService
        // 等到 broker 与 controller 交互选主成功之后，会将 fenced 设置为 false ,取消屏障。ha 相关组件全部拉起，主从身份已经确定，开始向 nameserver 注册
        // see : org.apache.rocketmq.broker.controller.ReplicasManager.startBasicService
        this.brokerController.setIsolated(fenced);
        this.brokerController.getMessageStore().getRunningFlags().makeFenced(fenced);
    }
}

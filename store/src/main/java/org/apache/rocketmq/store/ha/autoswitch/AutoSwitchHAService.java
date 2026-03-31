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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;
import org.rocksdb.RocksDBException;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService extends DefaultHAService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor(new ThreadFactoryImpl("AutoSwitchHAService_Executor_"));
    // 保存 slave 的 lastCaughtUpTimestamp
    // 当 slave 完全追上了 master 的进度，则更新 lastCaughtUpTimestamp
    // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.AbstractWriteSocketService.transferToSlave
    // caughtUpTimeMs 表示的是 slave 追上 master 进度那一刻的时间戳
    // 追上当前进度，caughtUpTimeMs 就是当前时间戳，追上上一次 transfer 时候的 master 进度，时间戳就是 lastTransferTimeMs
    private final ConcurrentHashMap<Long/*brokerId*/, Long/*lastCaughtUpTimestamp*/> connectionCaughtUpTimeTable = new ConcurrentHashMap<>();
    // org.apache.rocketmq.broker.controller.ReplicasManager.doReportSyncStateSetChanged
    private final List<Consumer<Set<Long/*brokerId*/>>> syncStateSetChangedListeners = new ArrayList<>();
    private final Set<Long/*brokerId*/> syncStateSet = new HashSet<>();
    private final Set<Long> remoteSyncStateSet = new HashSet<>();
    private final ReadWriteLock syncStateSetReadWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = syncStateSetReadWriteLock.readLock();
    private final Lock writeLock = syncStateSetReadWriteLock.writeLock();

    //  Indicate whether the syncStateSet is currently in the process of being synchronized to controller.
    private volatile boolean isSynchronizingSyncStateSet = false;

    private EpochFileCache epochCache;
    private AutoSwitchHAClient haClient;
    // controller 为 broker 分配的 id
    private Long localBrokerId = null;

    public AutoSwitchHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        // user.home/store/epochFileCheckpoint(默认)
        this.epochCache = new EpochFileCache(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        // 从 epochFileCheckpoint 文件中恢复 broker 的 epoch
        this.epochCache.initCacheFromFile();
        this.defaultMessageStore = defaultMessageStore;
        // 用于接收 slave 的连接，存储在 org.apache.rocketmq.store.ha.DefaultHAService.connectionList
        this.acceptSocketService = new AutoSwitchAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        // 向 slave 传输 commitlog
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.executorService.shutdown();
    }

    @Override
    public void removeConnection(HAConnection conn) {
        if (!defaultMessageStore.isShutdown()) {
            final Set<Long> syncStateSet = getLocalSyncStateSet();
            Long slave = ((AutoSwitchHAConnection) conn).getSlaveId();
            if (syncStateSet.contains(slave)) {
                syncStateSet.remove(slave);
                markSynchronizingSyncStateSet(syncStateSet);
                notifySyncStateSetChanged(syncStateSet);
            }
        }
        super.removeConnection(conn);
    }

    @Override
    public boolean changeToMaster(int masterEpoch) throws RocksDBException {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (masterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
            return false;
        }
        // 如果 broker 之前是 slave ,那么这里会销毁所有的 ha client 连接
        destroyConnections();
        // Stop ha client if needed
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        // Truncate dirty file
        // 由于现在本 broker 节点的 commitlog 中包含无效的 msg ,比如之前是 slave, 从 master 同步过来的日志包含不完整的 msg
        // 比如 master commitlog 中只同步过来半个 msg , 主从同步的粒度是字节，不是 msg, 所以 slave commitlog 中可能存在不完整的 msg (没来得及同步)
        // 这里需要将这些无效的消息截断掉，从新修正 commitlog 以及 consumer queue
        // 位于 truncateOffset 之后的消息全部截断
        final long truncateOffset = truncateInvalidMsg();
        // master 当前的 MaxPhyOffset 与所有 slave 中 ackOffset 的最小值
        this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());

        if (truncateOffset >= 0) {
            // truncateOffset 之后的 commitlog ， consume queue 已经被截断
            // 这里要修正 epoch ,将 truncateOffset 之后的 epoch 删除, 并持久化到 user.home/store/epochFileCheckpoint(默认) 文件中
            this.epochCache.truncateSuffixByOffset(truncateOffset);
        }

        // Append new epoch to epochFile
        // 为本次 master 的选举创建新的 epoch , startOffset 为当前 commitlog 的 MaxPhyOffset
        final EpochEntry newEpochEntry = new EpochEntry(masterEpoch, this.defaultMessageStore.getMaxPhyOffset());
        if (this.epochCache.lastEpoch() >= masterEpoch) {
            // 将 masterEpoch 之后的 epoch 删除
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
        }
        // 添加新的 epoch , 顺便会修正 epoch 之间 startOffset 与 endOffset 之间的连续
        // 前一个 epoch 的 endOffset 是后一个 epoch 的 startOffset
        // 最后一个 epoch 的 endOffset 是无限大
        this.epochCache.appendEntry(newEpochEntry);

        // Waiting consume queue dispatch
        while (defaultMessageStore.dispatchBehindBytes() > 0) {
            try {
                Thread.sleep(100);
            } catch (Exception ignored) {

            }
        }

        if (defaultMessageStore.isTransientStorePoolEnable()) {
            // 唤醒 commitRealTimeService 线程将 TransientStorePool 中的消息写入到 commitlog 中
            waitingForAllCommit();
            defaultMessageStore.getTransientStorePool().setRealCommit(true); // 是否实时提交
        }

        LOGGER.info("TruncateOffset is {}, confirmOffset is {}, maxPhyOffset is {}", truncateOffset, this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMaxPhyOffset());
        this.defaultMessageStore.recoverTopicQueueTable();
        this.defaultMessageStore.setStateMachineVersion(masterEpoch);
        LOGGER.info("Change ha to master success, newMasterEpoch:{}, startOffset:{}", masterEpoch, newEpochEntry.getStartOffset());
        return true;
    }

    @Override
    public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (newMasterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave", newMasterEpoch, lastEpoch);
            return false;
        }
        try {
            // 如果原来是 master , 这里要销毁所有 slave 的 ha connection
            destroyConnections();
            if (this.haClient == null) {
                // 创建 haClient , 向 master 同步日志
                this.haClient = new AutoSwitchHAClient(this, defaultMessageStore, this.epochCache, slaveId);
            } else {
                this.haClient.reOpen();
            }
            this.haClient.updateMasterAddress(newMasterAddr);
            this.haClient.updateHaMasterAddress(null);
            // 连接 master , ha 相关的 handshake , transfer 截断都是在这里处理
            this.haClient.start();

            if (defaultMessageStore.isTransientStorePoolEnable()) {
                waitingForAllCommit();
                defaultMessageStore.getTransientStorePool().setRealCommit(false);
            }

            this.defaultMessageStore.setStateMachineVersion(newMasterEpoch);

            LOGGER.info("Change ha to slave success, newMasterAddress:{}, newMasterEpoch:{}", newMasterAddr, newMasterEpoch);
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error happen when change ha to slave", e);
            return false;
        }
    }

    @Override
    public boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (masterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
            return false;
        }
        // Append new epoch to epochFile
        // 新 epoch 的 startOffet 是当前 commitlog 的最大 offset
        final EpochEntry newEpochEntry = new EpochEntry(masterEpoch, this.defaultMessageStore.getMaxPhyOffset());
        if (this.epochCache.lastEpoch() >= masterEpoch) {
            // 最后一个 epoch 的 endOffset 为无限大
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
        }
        this.epochCache.appendEntry(newEpochEntry);

        this.defaultMessageStore.setStateMachineVersion(masterEpoch);
        LOGGER.info("Change ha to master success, last role is master, newMasterEpoch:{}, startOffset:{}",
            masterEpoch, newEpochEntry.getStartOffset());
        return true;
    }

    @Override
    public boolean changeToSlaveWhenMasterNotChange(String newMasterAddr, int newMasterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (newMasterEpoch < lastEpoch) {
            LOGGER.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave", newMasterEpoch, lastEpoch);
            return false;
        }

        this.defaultMessageStore.setStateMachineVersion(newMasterEpoch);
        LOGGER.info("Change ha to slave success, master doesn't change, newMasterAddress:{}, newMasterEpoch:{}",
            newMasterAddr, newMasterEpoch);
        return true;
    }

    public void waitingForAllCommit() {
        while (getDefaultMessageStore().remainHowManyDataToCommit() > 0) {
            getDefaultMessageStore().getCommitLog().getFlushManager().wakeUpCommit();
            try {
                Thread.sleep(100);
            } catch (Exception e) {

            }
        }
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void updateMasterAddress(String newAddr) {
    }

    public void registerSyncStateSetChangedListener(final Consumer<Set<Long>> listener) {
        this.syncStateSetChangedListeners.add(listener);
    }

    public void notifySyncStateSetChanged(final Set<Long> newSyncStateSet) {
        this.executorService.submit(() -> {
            // org.apache.rocketmq.broker.controller.ReplicasManager.doReportSyncStateSetChanged
            syncStateSetChangedListeners.forEach(listener -> listener.accept(newSyncStateSet));
        });
        LOGGER.info("Notify the syncStateSet has been changed into {}.", newSyncStateSet);
    }

    /**
     * Check and maybe shrink the SyncStateSet.
     * A slave will be removed from SyncStateSet if (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public Set<Long> maybeShrinkSyncStateSet() {
        final Set<Long> newSyncStateSet = getLocalSyncStateSet();
        boolean isSyncStateSetChanged = false;
        // 15s
        final long haMaxTimeSlaveNotCatchup = this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        for (Map.Entry<Long, Long> next : this.connectionCaughtUpTimeTable.entrySet()) {
            final Long slaveBrokerId = next.getKey();
            if (newSyncStateSet.contains(slaveBrokerId)) {
                final Long lastCaughtUpTimeMs = next.getValue();
                // 如果 slave 的 lastCaughtUpTimeMs 超过 15s 没有更新，那么就会被踢出 SyncStateSet
                // master 会不断地 transfer 日志到 slave , 即使没有日志可 transfer(slave caught up), 也会更新这里的 lastCaughtUpTimeMs
                if ((System.currentTimeMillis() - lastCaughtUpTimeMs) > haMaxTimeSlaveNotCatchup) {
                    newSyncStateSet.remove(slaveBrokerId);
                    isSyncStateSetChanged = true;
                }
            }
        }

        // If the slaveBrokerId is in syncStateSet but not in connectionCaughtUpTimeTable,
        // it means that the broker has not connected.
        Iterator<Long> iterator = newSyncStateSet.iterator();
        while (iterator.hasNext()) {
            Long slaveBrokerId = iterator.next();
            if (!Objects.equals(slaveBrokerId, this.localBrokerId) && !this.connectionCaughtUpTimeTable.containsKey(slaveBrokerId)) {
                // 剔除不活跃的 slave
                iterator.remove();
                isSyncStateSetChanged = true;
            }
        }

        if (isSyncStateSetChanged) {
            markSynchronizingSyncStateSet(newSyncStateSet);
        }
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to SyncStateSet. A slave will be added to SyncStateSet if its slaveMaxOffset >=
     * current confirmOffset, and it is caught up to an offset within the current leader epoch.
     */
    public void maybeExpandInSyncStateSet(final Long slaveBrokerId, final long slaveMaxOffset) {
        final Set<Long> currentSyncStateSet = getLocalSyncStateSet();
        if (currentSyncStateSet.contains(slaveBrokerId)) {
            return;
        }
        // 当前 SyncStateSet 中所有 slave 的 MaxOffset 的最⼩值
        final long confirmOffset = this.defaultMessageStore.getConfirmOffset();
        if (slaveMaxOffset >= confirmOffset) {
            final EpochEntry currentLeaderEpoch = this.epochCache.lastEntry();
            if (slaveMaxOffset >= currentLeaderEpoch.getStartOffset()) {
                LOGGER.info("The slave {} has caught up, slaveMaxOffset: {}, confirmOffset: {}, epoch: {}, leader epoch startOffset: {}.",
                    slaveBrokerId, slaveMaxOffset, confirmOffset, currentLeaderEpoch.getEpoch(), currentLeaderEpoch.getStartOffset());
                currentSyncStateSet.add(slaveBrokerId);
                markSynchronizingSyncStateSet(currentSyncStateSet);
                // Notify the upper layer that syncStateSet changed.
                notifySyncStateSetChanged(currentSyncStateSet);
            }
        }
    }

    private void markSynchronizingSyncStateSet(final Set<Long> newSyncStateSet) {
        this.writeLock.lock();
        try {
            // 当检测到 SyncStateSet 改变的时候会设置为 true
            // 当向 controller 同步完成之后会设置为 false
            this.isSynchronizingSyncStateSet = true;
            this.remoteSyncStateSet.clear();
            this.remoteSyncStateSet.addAll(newSyncStateSet);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void markSynchronizingSyncStateSetDone() {
        // No need to lock, because the upper-level calling method has already locked write lock
        this.isSynchronizingSyncStateSet = false;
    }

    public boolean isSynchronizingSyncStateSet() {
        return isSynchronizingSyncStateSet;
    }

    public void updateConnectionLastCaughtUpTime(final Long slaveBrokerId, final long lastCaughtUpTimeMs) {
        Long prevTime = ConcurrentHashMapUtils.computeIfAbsent(this.connectionCaughtUpTimeTable, slaveBrokerId, k -> 0L);
        this.connectionCaughtUpTimeTable.put(slaveBrokerId, Math.max(prevTime, lastCaughtUpTimeMs));
    }

    public void updateConfirmOffsetWhenSlaveAck(final Long slaveBrokerId) {
        this.readLock.lock();
        try {
            if (this.syncStateSet.contains(slaveBrokerId)) {
                this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        this.readLock.lock();
        try {
            if (this.isSynchronizingSyncStateSet) {
                return Math.max(this.syncStateSet.size(), this.remoteSyncStateSet.size());
            } else {
                return this.syncStateSet.size();
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            Set<Long> localSyncStateSet = getLocalSyncStateSet();
            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                cInfo.setInSync(localSyncStateSet.contains(((AutoSwitchHAConnection) conn).getSlaveId()));

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(localSyncStateSet.size() - 1);
        }
        return info;
    }
    // master 当前的 MaxPhyOffset 与所有 SyncStateSet 中的 slave  ackOffset 的最小值
    public long computeConfirmOffset() {
        final Set<Long> currentSyncStateSet = getSyncStateSet();
        long newConfirmOffset = this.defaultMessageStore.getMaxPhyOffset();
        // 所有 slave 的 brokerId
        List<Long> idList = this.connectionList.stream().map(connection -> ((AutoSwitchHAConnection)connection).getSlaveId()).collect(Collectors.toList());

        // To avoid the syncStateSet is not consistent with connectionList.
        // Fix issue: https://github.com/apache/rocketmq/issues/6662
        // slave 在 syncStateSet 中，却不在 idList（保存所有 slave 的 ha 连接）
        // 异常情况，slave ha 连接丢失，却仍然在 syncStateSet 中
        for (Long syncId : currentSyncStateSet) {
            if (!idList.contains(syncId) && this.localBrokerId != null && !Objects.equals(syncId, this.localBrokerId)) {
                LOGGER.warn("Slave {} is still in syncStateSet, but has lost its connection. So new offset can't be compute.", syncId);
                // Without check and re-compute, return the confirmOffset's value directly.
                // ConfirmOffset 直接计算的话就是 maxOffset (Directly)
                return this.defaultMessageStore.getConfirmOffsetDirectly();
            }
        }

        for (HAConnection connection : this.connectionList) {
            final Long slaveId = ((AutoSwitchHAConnection) connection).getSlaveId();
            // 必须存在于 syncStateSet
            if (currentSyncStateSet.contains(slaveId) && connection.getSlaveAckOffset() > 0) {
                newConfirmOffset = Math.min(newConfirmOffset, connection.getSlaveAckOffset());
            }
        }
        // master 当前的 MaxPhyOffset 与所有 SyncStateSet 中的 slave  ackOffset 的最小值
        return newConfirmOffset;
    }

    public void setSyncStateSet(final Set<Long> syncStateSet) {
        this.writeLock.lock();
        try {
            markSynchronizingSyncStateSetDone();
            this.syncStateSet.clear();
            this.syncStateSet.addAll(syncStateSet);
            this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Return the union of the local and remote syncStateSets
     */
    public Set<Long> getSyncStateSet() {
        this.readLock.lock();
        try {
            if (this.isSynchronizingSyncStateSet) {
                Set<Long> unionSyncStateSet = new HashSet<>(this.syncStateSet.size() + this.remoteSyncStateSet.size());
                unionSyncStateSet.addAll(this.syncStateSet);
                unionSyncStateSet.addAll(this.remoteSyncStateSet);
                return unionSyncStateSet;
            } else {
                HashSet<Long> syncStateSet = new HashSet<>(this.syncStateSet.size());
                syncStateSet.addAll(this.syncStateSet);
                return syncStateSet;
            }
        } finally {
            this.readLock.unlock();
        }
    }

    public Set<Long> getLocalSyncStateSet() {
        this.readLock.lock();
        try {
            HashSet<Long> localSyncStateSet = new HashSet<>(this.syncStateSet.size());
            localSyncStateSet.addAll(this.syncStateSet);
            return localSyncStateSet;
        } finally {
            this.readLock.unlock();
        }
    }

    public void truncateEpochFilePrefix(final long offset) {
        this.epochCache.truncatePrefixByOffset(offset);
    }

    public void truncateEpochFileSuffix(final long offset) {
        this.epochCache.truncateSuffixByOffset(offset);
    }

    /**
     * Try to truncate incomplete msg transferred from master.
     * 由于现在本 broker 节点的 commitlog 中包含无效的 msg ,比如之前是 slave, 从 master 同步过来的日志包含不完整的 msg
     * 比如 master commitlog 中只同步过来半个 msg , 主从同步的粒度是字节，不是 msg, 所以 slave commitlog 中可能存在不完整的 msg (没来得及同步)
     */
    public long truncateInvalidMsg() throws RocksDBException {
        // Get number of the bytes that have been stored in commit log and not yet dispatched to consume queue.
        long dispatchBehind = this.defaultMessageStore.dispatchBehindBytes();
        // commitlog 中的字节全部 dispatch 到 consumequeue 中，说明 commitlog 中的消息全部是完整的，不存在无效消息
        // 所以不需要截断
        if (dispatchBehind <= 0) {
            LOGGER.info("Dispatch complete, skip truncate");
            return -1;
        }

        boolean doNext = true;

        // Here we could use reputFromOffset in DefaultMessageStore directly.
        // reputFromOffset 之前的内容已经构建好相关索引了，需要检查的是 reputFromOffset 到 ConfirmOffset 这段内容
        // 如果出现无效的消息就截断
        long reputFromOffset = this.defaultMessageStore.getReputFromOffset();
        // 开始查找需要截断的位置
        do {
            // 查找 reputFromOffset 所在的 mappedFile, 返回 [reputFromOffset , ...) 范围的 buffer
            // 还没有 reput 的内容
            SelectMappedBufferResult result = this.defaultMessageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            try {
                // 全局，记住只要是 offset 就是全局的 commitlog offset
                reputFromOffset = result.getStartOffset();

                int readSize = 0;
                while (readSize < result.getSize()) {
                    // 校验 buffer 中未 reput 的消息内容
                    // 这里会检验一条消息，在整个 while 循环中会一条一条消息的校验，直到找到不完整的消息（也就是从这条不完整的消息处开始截断）
                    DispatchRequest dispatchRequest = this.defaultMessageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                    if (dispatchRequest.isSuccess()) { // 完整的消息，继续向后校验
                        // 完整的消息 size
                        int size = dispatchRequest.getMsgSize();
                        if (size > 0) {
                            // 有效消息 size 累加，直到找到无效的消息停止累加，而 reputFromOffset 就是截断位置，其之后的内容全部是无效的
                            reputFromOffset += size;
                            readSize += size;
                        } else {
                            // 现在 reputFromOffset 所在的 commitlog 已经校验完了，全部有效
                            // 开始校验下一个 commitlog
                            reputFromOffset = this.defaultMessageStore.getCommitLog().rollNextFile(reputFromOffset);
                            break;
                        }
                    } else { // 找到不完整的消息
                        // 校验到此为止，已经找到截断位置了，就是累加之后的 reputFromOffset
                        // 此时 reputFromOffset 之后的消息都是无效的
                        doNext = false;
                        break;
                    }
                }
            } finally {
                result.release();
            }
        } while (reputFromOffset < this.defaultMessageStore.getMaxPhyOffset() && doNext);
        // 由于在校验 commitlog 中的消息的时候，reputFromOffset 也会增加
        // 所以这里的 reputFromOffset 指的是 commitlog 中有效的 msg offset
        LOGGER.info("Truncate commitLog to {}", reputFromOffset);
        // 这里会将无效的 msg 都截断掉（reputFromOffset 之前的全都是已经校验过的有效 msg）
        // 涉及截断 commit log 和 consume queue
        this.defaultMessageStore.truncateDirtyFiles(reputFromOffset);
        return reputFromOffset;
    }

    public int getLastEpoch() {
        return this.epochCache.lastEpoch();
    }

    public List<EpochEntry> getEpochEntries() {
        return this.epochCache.getAllEntries();
    }

    public Long getLocalBrokerId() {
        return localBrokerId;
    }

    public void setLocalBrokerId(Long localBrokerId) {
        this.localBrokerId = localBrokerId;
    }

    class AutoSwitchAcceptSocketService extends AcceptSocketService {

        public AutoSwitchAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return AutoSwitchAcceptSocketService.class.getSimpleName();
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new AutoSwitchHAConnection(AutoSwitchHAService.this, sc, AutoSwitchHAService.this.epochCache);
        }
    }
}

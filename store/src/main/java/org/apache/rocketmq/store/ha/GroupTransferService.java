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

package org.apache.rocketmq.store.ha;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

/**
 * GroupTransferService Service
 *     在 auto ha switch 的模式下，master 的 commitlog 是通过 ha 传输到 slave
 *     这里会等待 ha 的传输，如果 requestsRead 队列中，请求传输的内容已经通过 ha 传过去了 transferOK = ok ，那么就无需等待了
 *     如果还没有就等待传输完成直到 timeout
 *     因为现在 slave 已经 ackOffset 回来了,see:org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.ReadSocketService.HAServerReader.processReadResult
 *     所以要唤醒 groupTransferServer 去检查生产者写到master的commitlog是否
 *     传送到了slave,因为slave每次接收到master的transfer
 *     之后都会执行这里的 ackOffset 流程，如果ackOffset大于
 *     这里的push2SlaveMaxOffset，就会调用notifyTransferSome方法唤醒 groupTransferServer
 *     在doWaitTransfer方法中根据生产者每次写入master需要同步的
 *     slave个数，是同步一个？还是SyncStateSet中的slave都需要同步
 *     来判断生产者的消息是否同步到了应有的slave个数
 *     req.getNextOffset 表示生产者写入到commitlog的offset
 *     slave ack 的offset需要达到getNextOffset才算同步到slave
 *     只要ack的slave个数满足要求就算写入消息成功
 */
public class GroupTransferService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private final DefaultMessageStore defaultMessageStore;
    private final HAService haService;
    private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile List<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

    public GroupTransferService(final HAService haService, final DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        wakeup();
    }

    public void notifyTransferSome() {
        this.notifyTransferObject.wakeup();
    }

    private void swapRequests() {
        lock.lock();
        try {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }
    // 在 auto ha switch 的模式下，master 的 commitlog 是通过 ha 传输到 slave
    // 这里会等待 ha 的传输，如果 requestsRead 队列中，请求传输的内容已经通过 ha 传过去了 transferOK = ok ，那么就无需等待了
    // 如果还没有就等待传输完成直到 timeout
    // 因为现在 slave 已经 ackOffset 回来了,see:org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.ReadSocketService.HAServerReader.processReadResult
    // 所以要唤醒 groupTransferServer 去检查生产者写到master的commitlog是否
    // 传送到了slave,因为slave每次接收到master的transfer
    // 之后都会执行这里的 ackOffset 流程，如果ackOffset大于
    // 这里的push2SlaveMaxOffset，就会调用notifyTransferSome方法唤醒 groupTransferServer
    // 在doWaitTransfer方法中根据生产者每次写入master需要同步的
    // slave个数，是同步一个？还是SyncStateSet中的slave都需要同步
    // 来判断生产者的消息是否同步到了应有的slave个数
    // req.getNextOffset 表示生产者写入到commitlog的offset
    // slave ack 的offset需要达到getNextOffset才算同步到slave
    // 只要ack的slave个数满足要求就算写入消息成功
    private void doWaitTransfer() { // 主要用于判断生产者的消息是否同步到了应有的slave个数
        if (!this.requestsRead.isEmpty()) {
            for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                boolean transferOK = false;

                long deadLine = req.getDeadLine();
                final boolean allAckInSyncStateSet = req.getAckNums() == MixAll.ALL_ACK_IN_SYNC_STATE_SET;
                // 不停的检查 GroupCommitRequest，直到 transferOK 或者 timeout
                for (int i = 0; !transferOK && deadLine - System.nanoTime() > 0; i++) {
                    if (i > 0) {
                        this.notifyTransferObject.waitForRunning(1);
                    }
                    // 不需要副本应答或者需要一个副本应答
                    if (!allAckInSyncStateSet && req.getAckNums() <= 1) {
                        // 要 Transfer 的日志已经通过 ha 传输过去了，这里就不需要在 Transfer 了
                        // 只要传输过去一个就可以了 transferOK = true
                        // Push2SlaveMaxOffset 会在每次 slave ackOffset 的时候被修改
                        // org.apache.rocketmq.store.ha.DefaultHAService.notifyTransferSome
                        transferOK = haService.getPush2SlaveMaxOffset().get() >= req.getNextOffset();
                        continue;
                    }

                    if (allAckInSyncStateSet && this.haService instanceof AutoSwitchHAService) {
                        // In this mode, we must wait for all replicas that in SyncStateSet.
                        final AutoSwitchHAService autoSwitchHAService = (AutoSwitchHAService) this.haService;
                        final Set<Long> syncStateSet = autoSwitchHAService.getSyncStateSet();
                        if (syncStateSet.size() <= 1) {
                            // Only master
                            transferOK = true;
                            break;
                        }

                        // Include master
                        int ackNums = 1;
                        // syncStateSet 中的 slave 全部需要应答
                        for (HAConnection conn : haService.getConnectionList()) {
                            final AutoSwitchHAConnection autoSwitchHAConnection = (AutoSwitchHAConnection) conn;
                            if (syncStateSet.contains(autoSwitchHAConnection.getSlaveId()) && autoSwitchHAConnection.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= syncStateSet.size()) {
                                transferOK = true;
                                break;
                            }
                        }
                    } else {
                        // Include master
                        int ackNums = 1;
                        // 指定应答的slave数量AckNums,不需要是 syncStateSet 中的 slave
                        for (HAConnection conn : haService.getConnectionList()) {
                            // TODO: We must ensure every HAConnection represents a different slave
                            // Solution: Consider assign a unique and fixed IP:ADDR for each different slave
                            if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= req.getAckNums()) {
                                transferOK = true;
                                break;
                            }
                        }
                    }
                }

                if (!transferOK) {
                    log.warn("transfer message to slave timeout, offset : {}, request acks: {}",
                        req.getNextOffset(), req.getAckNums());
                }

                req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
            }

            this.requestsRead = new LinkedList<>();
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                // 每隔10ms 检测 slave 的同步情况
                this.waitForRunning(10);
                this.doWaitTransfer();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + GroupTransferService.class.getSimpleName();
        }
        return GroupTransferService.class.getSimpleName();
    }
}

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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.ha.io.HAWriter;

public class AutoSwitchHAConnection implements HAConnection {

    /**
     * Handshake data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬────────────────────────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   EpochEntrySize * EpochEntryNums  │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (12bytes * EpochEntryNums)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┼────────────────────────────────────┤
     * │                       Header                            │             Body                   │
     * │                                                         │                                    │
     * </pre>
     * Handshake Header protocol Format:
     * current state + body size + offset + epoch
     */
    public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8 + 4;

    /**
     * Transfer data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬─────────────────────┬──────────────────┬──────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   epochStartOffset  │   confirmOffset  │    log data      │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (8bytes)       │      (8bytes)    │   (data size)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┴─────────────────────┴──────────────────┼──────────────────┤
     * │                                               Header                                             │       Body       │
     * │                                                                                                  │                  │
     * </pre>
     * Transfer Header protocol Format:
     * current state + body size + offset + epoch  + epochStartOffset + additionalInfo(confirmOffset)
     */
    public static final int TRANSFER_HEADER_SIZE = HANDSHAKE_HEADER_SIZE + 8 + 8;
    public static final int EPOCH_ENTRY_SIZE = 12; // 4（epoch） + 8 (startOffset)
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final AutoSwitchHAService haService;
    // slave 的 ha connection
    private final SocketChannel socketChannel;
    // slave broker 地址
    private final String clientAddress;
    // master 的 epochCache
    private final EpochFileCache epochCache;
    // 向 slave 发送 ha 响应
    private final AbstractWriteSocketService writeSocketService;
    // 接收 slave 的 ha 请求
    private final ReadSocketService readSocketService;
    private final FlowMonitor flowMonitor;

    private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;
    // 下一次的 request offset 是上一次 ack 的 offset
    private volatile long slaveRequestOffset = -1;
    // slave 向 master 报告其 commitlog 的最大 offset
    private volatile long slaveAckOffset = -1;
    /**
     * Whether the slave have already sent a handshake message
     * slave 通过 ha 发送 handshake 之后，master 处理 handshake 时会将 isSlaveSendHandshake 设置为 true
     * org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.ReadSocketService.HAServerReader#processReadResult(java.nio.ByteBuffer)
     *
     * WriteSocketService 如果检查到 isSlaveSendHandshake = true, 就向 slave 发送 handshake 响应
     * org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.AbstractWriteSocketService#run()
     */
    private volatile boolean isSlaveSendHandshake = false;
    private volatile int currentTransferEpoch = -1;
    private volatile long currentTransferEpochEndOffset = 0;
    private volatile boolean isSyncFromLastFile = false;
    private volatile boolean isAsyncLearner = false;
    private volatile long slaveId = -1;

    /**
     * Last endOffset(maxPhyOffset) when master transfer data to slave
     * 上一次向 slave transfer 的时候， master commitlog 最大 offset
     */
    private volatile long lastMasterMaxOffset = -1;
    /**
     * Last time ms when transfer data to slave.
     */
    private volatile long lastTransferTimeMs = 0;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, SocketChannel socketChannel,
        EpochFileCache epochCache) throws IOException {
        this.haService = haService;
        // slave 的 ha connection
        this.socketChannel = socketChannel;
        // master 的 epochCache
        this.epochCache = epochCache;
        // slave broker 地址
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        // 向 slave 发送 ha 响应
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        // 接收 slave 的 ha 请求
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.HANDSHAKE);
        this.flowMonitor.start();
        // 处理 slave ha connection 上的读请求
        this.readSocketService.start();
        // 处理 slave ha connection 上的写请求
        this.writeSocketService.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown(true);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    @Override
    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (final IOException e) {
                LOGGER.error("", e);
            }
        }
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
        this.currentState = connectionState;
    }

    public long getSlaveId() {
        return slaveId;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return this.writeSocketService.getNextTransferFromWhere();
    }

    private void changeTransferEpochToNext(final EpochEntry entry) {
        this.currentTransferEpoch = entry.getEpoch();
        this.currentTransferEpochEndOffset = entry.getEndOffset();
        if (entry.getEpoch() == this.epochCache.lastEpoch()) {
            // Use -1 to stand for Long.max
            this.currentTransferEpochEndOffset = -1;
        }
    }

    public boolean isAsyncLearner() {
        return isAsyncLearner;
    }

    public boolean isSyncFromLastFile() {
        return isSyncFromLastFile;
    }
    // 每次向 slave transfer 的时候，master 都会更新这里
    private synchronized void updateLastTransferInfo() {
        // 上一次向 slave transfer 的时候， master commitlog 最大 offset
        this.lastMasterMaxOffset = this.haService.getDefaultMessageStore().getMaxPhyOffset();
        this.lastTransferTimeMs = System.currentTimeMillis();
    }

    private synchronized void maybeExpandInSyncStateSet(long slaveMaxOffset) {
        if (!this.isAsyncLearner && slaveMaxOffset >= this.lastMasterMaxOffset) {
            // 如果 slave 追赶上了 master 的进度，那么 caughtUpTimeMs 就设置为当前时间
            // 如果只是 lastMasterMaxOffset ， 那么就设置为 lastTransferTimeMs(表示的是 slave 追上的只是上一次 transfer 时候 master 的进度，而不是当前 master 的进度)
            // caughtUpTimeMs 表示的是 slave 追上 master 进度那一刻的时间戳
            // 追上当前进度，caughtUpTimeMs 就是当前时间戳，追上上一次 transfer 时候的 master 进度，时间戳就是 lastTransferTimeMs
            long caughtUpTimeMs = this.haService.getDefaultMessageStore().getMaxPhyOffset() == slaveMaxOffset ? System.currentTimeMillis() : this.lastTransferTimeMs;
            // 更新 slave 的 LastCaughtUpTime
            this.haService.updateConnectionLastCaughtUpTime(this.slaveId, caughtUpTimeMs);
            // if its slaveMaxOffset >= current confirmOffset, and it is caught up to an offset within the current leader epoch.
            this.haService.maybeExpandInSyncStateSet(this.slaveId, slaveMaxOffset);
        }
    }

    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE); // 1M
        private final AbstractHAReader haReader;
        // 每一次处理 readBuffer 时候的处理标记，标记读取到了哪个位置，每次处理完就会清0，准备下一次处理
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
            haReader = new HAServerReader();
            haReader.registerHook(readSize -> {
                if (readSize > 0) {
                    ReadSocketService.this.lastReadTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.haReader.read(this.socketChannel, this.byteBufferRead);
                    if (!ok) {
                        AutoSwitchHAConnection.LOGGER.error("processReadEvent error");
                        break;
                    }
                    // ha 连接上超过 20s 空闲，无数据传输，那么就销毁
                    long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        LOGGER.warn("ha housekeeping, found this connection[" + clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            changeCurrentState(HAConnectionState.SHUTDOWN);

            writeSocketService.makeStop();

            haService.removeConnection(AutoSwitchHAConnection.this);

            haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }

            flowMonitor.shutdown(true);

            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        class HAServerReader extends AbstractHAReader {
            @Override
            protected boolean processReadResult(ByteBuffer byteBufferRead) {
                while (true) {
                    boolean processSuccess = true;
                    // position 之前都是读取到的内容
                    int readSocketPos = byteBufferRead.position();
                    // 还未处理的内容，这里会进行粘包拆包的处理
                    int diff = byteBufferRead.position() - ReadSocketService.this.processPosition;
                    // 先判断读取到的内容是否满足最小 size , 即 ha 协议的各个阶段的 header size (handshake or transfer)
                    // 如果 byteBufferRead 中的数据无法满足一个数据包的大小，那么就不做处理，数据仍然积攒在 byteBufferRead 中
                    // 等待下一次从 socket 中读取
                    if (diff >= AutoSwitchHAClient.MIN_HEADER_SIZE) {
                        // 开始从这里进行读取
                        int readPosition = ReadSocketService.this.processPosition;
                        // 无论 ha 的哪个阶段，ha 协议开头的 4 个字节都是 Current state 代表当前的 HAConnectionState
                        HAConnectionState slaveState = HAConnectionState.values()[byteBufferRead.getInt(readPosition)];

                        switch (slaveState) {
                            case HANDSHAKE:
                                // SlaveBrokerId
                                Long slaveBrokerId = byteBufferRead.getLong(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 8);
                                AutoSwitchHAConnection.this.slaveId = slaveBrokerId;
                                // Flag(isSyncFromLastFile)
                                short syncFromLastFileFlag = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 12);
                                if (syncFromLastFileFlag == 1) {
                                    AutoSwitchHAConnection.this.isSyncFromLastFile = true;
                                }
                                // Flag(isAsyncLearner role)
                                short isAsyncLearner = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 10);
                                if (isAsyncLearner == 1) {
                                    AutoSwitchHAConnection.this.isAsyncLearner = true;
                                }

                                isSlaveSendHandshake = true;
                                // 恢复 buffer 的 position 到未读取状态
                                byteBufferRead.position(readSocketPos);
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE;
                                LOGGER.info("Receive slave handshake, slaveBrokerId:{}, isSyncFromLastFile:{}, isAsyncLearner:{}",
                                    AutoSwitchHAConnection.this.slaveId, AutoSwitchHAConnection.this.isSyncFromLastFile, AutoSwitchHAConnection.this.isAsyncLearner);
                                break;
                            case TRANSFER:
                                // slave 向 master 报告当前 slave commitlog 的最大 offset
                                // 第一次报告是 slave 处理完 master 的 handshake 响应，截断完日志之后
                                // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAClient.doTruncate
                                long slaveMaxOffset = byteBufferRead.getLong(readPosition + 4);
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.TRANSFER_HEADER_SIZE;

                                AutoSwitchHAConnection.this.slaveAckOffset = slaveMaxOffset;
                                if (slaveRequestOffset < 0) {
                                    // 下一次的 request offset 是上一次 ack 的 offset
                                    slaveRequestOffset = slaveMaxOffset;
                                }
                                byteBufferRead.position(readSocketPos);
                                // 判断是否将该 slave 加入到 SyncStateSet 中并向 conterller 发起改变 SyncStateSet

                                // shrink 是由定时任务 org.apache.rocketmq.broker.controller.ReplicasManager.schedulingCheckSyncStateSet
                                // 检查各个 slave 的 LastCaughtUpTime ， 决定是否 shrink
                                // 更新 LastCaughtUpTime 的时机是在 transferToSlave ， master 发现已经没有 commitlog 可 transfer 到 slave 了
                                // 说明 slave 已经追上了 master 的进度，更新其 transferToSlave
                                // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.AbstractWriteSocketService.transferToSlave
                                maybeExpandInSyncStateSet(slaveMaxOffset);
                                // ConfirmOffset 为 ： master 当前的 MaxPhyOffset 与所有 SyncStateSet 中的 slave  ackOffset 的最小值
                                AutoSwitchHAConnection.this.haService.updateConfirmOffsetWhenSlaveAck(AutoSwitchHAConnection.this.slaveId);
                                // 通知 GroupTransferService doWaitTransfer ？
                                // 消息被写入 master 之后，通过 GroupTransferService 异步 Transfer 到 slave ?

                                // 在 auto ha switch 的模式下，master 的 commitlog 是通过 ha 传输到 slave
                                // 这里会等待 ha 的传输，如果 requestsRead 队列中，请求传输的内容已经通过 ha 传过去了 transferOK = ok ，那么就无需等待了
                                // 如果还没有就等待传输完成直到 timeout

                                // GroupTransferService 本身并不会传输 commitlog, 只是等待 ha 传输，检查 Transfer 的情况
                                AutoSwitchHAConnection.this.haService.notifyTransferSome(AutoSwitchHAConnection.this.slaveAckOffset);
                                break;
                            default:
                                LOGGER.error("Current state illegal {}", currentState);
                                return false;
                        }

                        if (!slaveState.equals(currentState)) {
                            LOGGER.warn("Master change state from {} to {}", currentState, slaveState);
                            // slave 向 master 第一次报告 ack offset 时， master 的状态在这里变为 TRANSFER
                            changeCurrentState(slaveState);
                        }
                        if (processSuccess) {
                            continue;
                        }
                    }

                    if (!byteBufferRead.hasRemaining()) {
                        byteBufferRead.position(ReadSocketService.this.processPosition);
                        // 将未处理的信息（position , limit）之间的数据移到最前面
                        byteBufferRead.compact();
                        // 清零，准备下一次从头再处理
                        ReadSocketService.this.processPosition = 0;
                    }
                    break;
                }

                return true;
            }
        }
    }

    class WriteSocketService extends AbstractWriteSocketService {
        // 封装的是某次需要传输到 slave 的 commitlog
        // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.WriteSocketService.getNextTransferDataSize
        private SelectMappedBufferResult selectMappedBufferResult;

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            super(socketChannel);
        }

        @Override
        protected int getNextTransferDataSize() {
            // 获取 nextTransferFromWhere 所在 commitlog 文件
            // selectResult 的起始是 nextTransferFromWhere 位置处，末尾是整个文件
            SelectMappedBufferResult selectResult = haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
            if (selectResult == null || selectResult.getSize() <= 0) {
                return 0;
            }
            this.selectMappedBufferResult = selectResult;
            return selectResult.getSize();
        }

        @Override
        protected void releaseData() {
            this.selectMappedBufferResult.release();
            this.selectMappedBufferResult = null;
        }

        @Override
        protected boolean transferData(int maxTransferSize) throws Exception {
            // selectMappedBufferResult 封装的是本次需要传输到 slave 的 commitlog
            // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.WriteSocketService.getNextTransferDataSize
            if (null != this.selectMappedBufferResult && maxTransferSize >= 0) {
                // 当有 body 传输的时候，selectMappedBufferResult 不为 null, 传输完会设置为 null
                this.selectMappedBufferResult.getByteBuffer().limit(maxTransferSize);
            }

            // Write Header
            boolean result = haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }

            if (null == this.selectMappedBufferResult) {
                return true;
            }

            // Write Body
            result = haWriter.write(this.socketChannel, this.selectMappedBufferResult.getByteBuffer());

            if (result) {
                // 每次传输完会 release 并置 null
                releaseData();
            }
            return result;
        }

        @Override
        protected void onStop() {
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }
    }

    abstract class AbstractWriteSocketService extends ServiceThread {
        protected final Selector selector;
        protected final SocketChannel socketChannel;
        protected final HAWriter haWriter;

        protected final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
        // Store master epochFileCache: (Epoch + startOffset) * 1000
        private final ByteBuffer handShakeBuffer = ByteBuffer.allocate(EPOCH_ENTRY_SIZE * 1000);
        protected long nextTransferFromWhere = -1;
        protected boolean lastWriteOver = true;
        protected long lastWriteTimestamp = System.currentTimeMillis();
        protected long lastPrintTimestamp = System.currentTimeMillis();
        protected long transferOffset = 0;

        public AbstractWriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
            haWriter = new HAWriter();
            haWriter.registerHook(writeSize -> {
                flowMonitor.addByteCountTransferred(writeSize);
                if (writeSize > 0) {
                    AbstractWriteSocketService.this.lastWriteTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        public long getNextTransferFromWhere() {
            return this.nextTransferFromWhere;
        }

        private boolean buildHandshakeBuffer() {
            // 获取 master 的所有 EpochEntry
            final List<EpochEntry> epochEntries = AutoSwitchHAConnection.this.epochCache.getAllEntries();
            // 当前 master 的 epoch
            final int lastEpoch = AutoSwitchHAConnection.this.epochCache.lastEpoch();
            // 当前 master commitlog 的最大 offset
            final long maxPhyOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getMaxPhyOffset();
            this.byteBufferHeader.position(0);
            // master 在 handshake 阶段响应的报文头
            this.byteBufferHeader.limit(HANDSHAKE_HEADER_SIZE);
            // State
            this.byteBufferHeader.putInt(currentState.ordinal());
            // Body size （master epochEntries 总大小）
            this.byteBufferHeader.putInt(epochEntries.size() * EPOCH_ENTRY_SIZE);
            // Offset
            this.byteBufferHeader.putLong(maxPhyOffset);
            // Epoch
            this.byteBufferHeader.putInt(lastEpoch);
            this.byteBufferHeader.flip();

            // EpochEntries，master 在 handshake 阶段响应报文 body
            this.handShakeBuffer.position(0);
            this.handShakeBuffer.limit(EPOCH_ENTRY_SIZE * epochEntries.size());
            for (final EpochEntry entry : epochEntries) {
                if (entry != null) {
                    this.handShakeBuffer.putInt(entry.getEpoch());
                    this.handShakeBuffer.putLong(entry.getStartOffset());
                }
            }
            this.handShakeBuffer.flip();
            LOGGER.info("Master build handshake header: maxEpoch:{}, maxOffset:{}, epochEntries:{}", lastEpoch, maxPhyOffset, epochEntries);
            return true;
        }

        private boolean handshakeWithSlave() throws IOException {
            // Write Header
            boolean result = this.haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }

            // Write Body
            return this.haWriter.write(this.socketChannel, this.handShakeBuffer);
        }

        // Normal transfer method
        private void buildTransferHeaderBuffer(long nextOffset, int bodySize) {

            EpochEntry entry = AutoSwitchHAConnection.this.epochCache.getEntry(AutoSwitchHAConnection.this.currentTransferEpoch);

            if (entry == null) {

                // If broker is started on empty disk and no message entered (nextOffset = -1 and currentTransferEpoch = -1), do not output error log when sending heartbeat
                if (nextOffset != -1 || currentTransferEpoch != -1 || bodySize > 0) {
                    LOGGER.error("Failed to find epochEntry with epoch {} when build msg header", AutoSwitchHAConnection.this.currentTransferEpoch);
                }

                if (bodySize > 0) {
                    return;
                }
                // Maybe it's used for heartbeat
                entry = AutoSwitchHAConnection.this.epochCache.firstEntry();
            }
            // Build Header
            this.byteBufferHeader.position(0);
            this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
            // State
            this.byteBufferHeader.putInt(currentState.ordinal());
            // Body size
            this.byteBufferHeader.putInt(bodySize);
            // Offset 当前这一批次的日志的起始偏移量
            this.byteBufferHeader.putLong(nextOffset);
            // Epoch
            this.byteBufferHeader.putInt(entry.getEpoch());
            // EpochStartOffset
            this.byteBufferHeader.putLong(entry.getStartOffset());
            // Additional info(confirm offset)
            final long confirmOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getConfirmOffset();
            this.byteBufferHeader.putLong(confirmOffset);
            this.byteBufferHeader.flip();
        }

        private boolean sendHeartbeatIfNeeded() throws Exception {
            // 距离上一次写操作的时间间隔
            long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
            // 写操作间隔超过了心跳 5s ，那么就 transferData，就当发送心跳了
            if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                // 构建 TransferHeader ， body size 为 0 ，不传输任何日志
                // 只是通过 TransferHeader 和 slave 同步 header 中的数据（epoch , confirmOffset）
                buildTransferHeaderBuffer(this.nextTransferFromWhere, 0);
                // 发送 TransferHeader
                return this.transferData(0);
            }
            return true;
        }

        private void transferToSlave() throws Exception {
            // 上一批次的 transfer 写操作是否写完
            if (this.lastWriteOver) {
                // 如果超过心跳间隔时间（5s）没有执行过写操作了，那么就 transferData （就当发送心跳了）
                // 只发送 TransferHeader ， body size 为 0 ，不传输任何日志
                // 只是通过 TransferHeader 和 slave 同步 header 中的数据（epoch , confirmOffset）
                this.lastWriteOver = sendHeartbeatIfNeeded();
            } else {
                // maxTransferSize == -1 means to continue transfer remaining data.
                // 上一批次没有写完，则继续传输剩下的数据
                this.lastWriteOver = this.transferData(-1);
            }
            if (!this.lastWriteOver) {
                return;
            }
            // 本次要传输的 commitlog size , 查找 nextTransferFromWhere 所在 commitlog 文件
            // 获取 mappedBuffer , 起始为 nextTransferFromWhere 位置，末尾是文件尾部
            int size = this.getNextTransferDataSize();
            if (size > 0) {
                // 每批传输日志的最大 size : 32k
                if (size > haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                    size = haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                }
                int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                if (size > canTransferMaxBytes) {
                    if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                        LOGGER.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                            String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                            String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                        lastPrintTimestamp = System.currentTimeMillis();
                    }
                    size = canTransferMaxBytes;
                }
                if (size <= 0) {
                    this.releaseData();
                    this.waitForRunning(100);
                    return;
                }

                // We must ensure that the transmitted logs are within the same epoch
                // If currentEpochEndOffset == -1, means that currentTransferEpoch = last epoch, so the endOffset = Long.max
                final long currentEpochEndOffset = AutoSwitchHAConnection.this.currentTransferEpochEndOffset;
                if (currentEpochEndOffset != -1 && this.nextTransferFromWhere + size > currentEpochEndOffset) {
                    final EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.nextEntry(AutoSwitchHAConnection.this.currentTransferEpoch);
                    if (epochEntry == null) {
                        LOGGER.error("Can't find a bigger epochEntry than epoch {}", AutoSwitchHAConnection.this.currentTransferEpoch);
                        waitForRunning(100);
                        return;
                    }
                    size = (int) (currentEpochEndOffset - this.nextTransferFromWhere);
                    changeTransferEpochToNext(epochEntry);
                }

                this.transferOffset = this.nextTransferFromWhere;
                this.nextTransferFromWhere += size;
                updateLastTransferInfo();

                // Build Header
                buildTransferHeaderBuffer(this.transferOffset, size);

                this.lastWriteOver = this.transferData(size);
            } else {
                // If size == 0, we should update the lastCatchupTimeMs
                // 此时说明 slave 已经完全追赶上了 master 的进度，更新 slave 的 LastCaughtUpTime
                // 由定时任务 org.apache.rocketmq.broker.controller.ReplicasManager.schedulingCheckSyncStateSet
                // 检查各个 slave 的 LastCaughtUpTime ， 决定是否 shrink
                AutoSwitchHAConnection.this.haService.updateConnectionLastCaughtUpTime(AutoSwitchHAConnection.this.slaveId, System.currentTimeMillis());
                // 当前线程在这里 wait 100 ms
                haService.getWaitNotifyObject().allWaitForRunning(100);
            }
        }

        @Override
        public void run() {
            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    // currentState 初始状态就是 HANDSHAKE
                    switch (currentState) {
                        case HANDSHAKE:
                            // Wait until the slave send it handshake msg to master.
                            // slave 通过 ha 发送 handshake 之后，master 处理 handshake 时会将 isSlaveSendHandshake 设置为 true
                            if (!isSlaveSendHandshake) {
                                this.waitForRunning(10);
                                continue;
                            }
                            // 表示上一次的发送操作是否发送完成（有可能 socket 写满了，导致发送不出去）
                            // 这次重新发送
                            if (this.lastWriteOver) {
                                // 构建 master 在 handshake 阶段的响应报文 ：byteBufferHeader（HEADER） + handShakeBuffer(BODY)
                                if (!buildHandshakeBuffer()) {
                                    LOGGER.error("AutoSwitchHAConnection build handshake buffer failed");
                                    this.waitForRunning(5000);
                                    continue;
                                }
                            }
                            // 向 slave 发送 handshake 响应
                            this.lastWriteOver = handshakeWithSlave();
                            if (this.lastWriteOver) {
                                // change flag to {false} to wait for slave notification
                                isSlaveSendHandshake = false;
                            }
                            break;
                        case TRANSFER:
                            // 在 slave ack offset  的时候会将 slaveRequestOffset 设置为 slave ack offset ，状态设置为 TRANSFER
                            // org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection.ReadSocketService.HAServerReader.processReadResult
                            if (-1 == slaveRequestOffset) {
                                this.waitForRunning(10);
                                continue;
                            }
                            // 初始为 -1
                            if (-1 == this.nextTransferFromWhere) {
                                if (0 == slaveRequestOffset) {
                                    // We must ensure that the starting point of syncing log
                                    // must be the startOffset of a file (maybe the last file, or the minOffset)
                                    final MessageStoreConfig config = haService.getDefaultMessageStore().getMessageStoreConfig();
                                    if (AutoSwitchHAConnection.this.isSyncFromLastFile) {
                                        long masterOffset = haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                                        masterOffset = masterOffset - (masterOffset % config.getMappedFileSizeCommitLog());
                                        if (masterOffset < 0) {
                                            masterOffset = 0;
                                        }
                                        this.nextTransferFromWhere = masterOffset;
                                    } else {
                                        this.nextTransferFromWhere = haService.getDefaultMessageStore().getCommitLog().getMinOffset();
                                    }
                                } else {
                                    // 从上一次 slave ack offset 处开始继续 Transfer
                                    this.nextTransferFromWhere = slaveRequestOffset;
                                }

                                // nextTransferFromWhere is not found. It may be empty disk and no message is entered
                                if (this.nextTransferFromWhere == -1) {
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                // Setup initial transferEpoch
                                // 根据 nextTransferFromWhere 查找其所在的 epoch 范围
                                EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.findEpochEntryByOffset(this.nextTransferFromWhere);
                                if (epochEntry == null) {
                                    LOGGER.error("Failed to find an epochEntry to match nextTransferFromWhere {}", this.nextTransferFromWhere);
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                changeTransferEpochToNext(epochEntry);
                                LOGGER.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}",
                                    AutoSwitchHAConnection.this.clientAddress, this.nextTransferFromWhere, epochEntry);
                            }
                            // master 会在循环里不停地传输 commitlog, 直到传输完毕
                            transferToSlave();
                            break;
                        default:
                            throw new Exception("unexpected state " + currentState);
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.onStop();

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(AutoSwitchHAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }

            flowMonitor.shutdown(true);

            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        abstract protected int getNextTransferDataSize();

        abstract protected void releaseData();

        abstract protected boolean transferData(int maxTransferSize) throws Exception;

        abstract protected void onStop();
    }
}

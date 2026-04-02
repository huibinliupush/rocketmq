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
package org.apache.rocketmq.store;

import com.google.common.base.Strings;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.exception.StoreException;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.lock.AdaptiveBackOffSpinLockImpl;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.util.LibC;
import org.rocksdb.RocksDBException;

import sun.nio.ch.DirectBuffer;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog implements Swappable {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string + PROPERTY_SEPARATOR]
     */
    public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore defaultMessageStore;

    private final FlushManager flushManager;
    private final ColdDataCheckService coldDataCheckService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;
    // from StoreCheckpoint
    // see : org.apache.rocketmq.store.StoreCheckpoint.StoreCheckpoint
    // 主节点的 confirmOffset 是 master 当前的 MaxPhyOffset 与所有 SyncStateSet 中的 slave  ackOffset 的最小值
    // 从节点的 confirmOffset 由两个值决定：
    // 一个是主节点发送消息时，Header 中包含的当前 confirmOffset；另一个是当前的最大 confirmOffset。取这两个值中的最小值
    // slave 的 ConfirmOffset 为 master 的 confirmOffset 与 slave commitlog 最大 offset 的最小值

    // 在 recover 的时候,起始 confirmOffset：
    // controller 模式:已经被构建索引的最大消息 offset ,位于 offset 之前的消息全部被构建索引了
    // 非 controller 模式就是 lastValidMsgPhyOffset
    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;
    // PutMessageReentrantLock
    protected final PutMessageLock putMessageLock;

    protected final TopicQueueLock topicQueueLock;
    // 这里的文件路径所在的磁盘分区使用率均超过了规定的 DiskSpaceCleanForciblyRatio（85%）
    // see : org.apache.rocketmq.store.DefaultMessageStore.CleanCommitLogService.isSpaceToDelete
    // 由定时清理过期的 commitlog 任务设置
    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private final FlushDiskWatcher flushDiskWatcher;

    protected int commitLogSize;
    // false
    private final boolean enabledAppendPropCRC;

    public CommitLog(final DefaultMessageStore messageStore) {
        // user.home/stpre/commitlog
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            this.mappedFileQueue = new MappedFileQueue(storePath,
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = messageStore;

        this.flushManager = new DefaultFlushManager();
        this.coldDataCheckService = new ColdDataCheckService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig());
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig());
            }
        };

        PutMessageLock adaptiveBackOffSpinLock = new AdaptiveBackOffSpinLockImpl();
        // PutMessageReentrantLock
        this.putMessageLock = messageStore.getMessageStoreConfig().getUseABSLock() ? adaptiveBackOffSpinLock :
            messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

        this.flushDiskWatcher = new FlushDiskWatcher();

        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());

        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // false
        this.enabledAppendPropCRC = messageStore.getMessageStoreConfig().isEnabledAppendPropCRC();
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    public boolean load() {
        // 加载 store path/commitlog 下的所有文件
        boolean result = this.mappedFileQueue.load();
        // dataReadAheadEnable = true
        if (result && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable()) {
            // 取消预读， 默认开启预读
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.start();
        }
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            // offset 是全局的绝对位置
            // pos 是 mappedFile 内是文件内的局部相对位置
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();

        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }

        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                int readableSize = mappedFile.getReadPosition() - pos;
                int readSize = Math.min(remainSize, readableSize);

                SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
                if (bufferResult == null) {
                    break;
                }
                bufferResultList.add(bufferResult);
                remainSize -= readSize;
                startOffset += readSize;
            }
        }

        return bufferResultList;
    }

    public SelectMappedFileResult getFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int size = (int) (mappedFile.getReadPosition() - offset % mappedFileSize);
            if (size > 0) {
                return new SelectMappedFileResult(size, mappedFile);
            }
        }
        return null;
    }

    //Create new mappedFile if not exits.
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }

        return true;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     * maxPhyOffsetOfConsumeQueue: 当前被索引的最大 commitlog offset
     * 查找commitlog中最大有效的消息 offset,更新 confirmOffset, 以及 flushWhere,CommitWhere, 位于有效 offset 之后的消息全部截断
     * @throws RocksDBException only in rocksdb mode
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        // checkCRCOnRecover = true , 在 recover 的时候需要校验每条消息的 CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        // duplicationEnable = false
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        // 获取所有 commitlog files
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 1;
            while (index > 0) {
                // 从 mappedFile 的 FileFromOffset 如果小于等于当前被索引的最大 commitlog offset
                // 那么说明前面的所有 mappedFile 均为有效的，因为已经全部被构建索引了
                // 那么就从这个 mappedFile 开始向后一个一个的校验
                MappedFile mappedFile = mappedFiles.get(index);
                // 这也是为什么要先 recover consume queue 的原因，目的就是获取 maxPhyOffsetOfConsumeQueue
                if (mappedFile.getFileFromOffset() <= maxPhyOffsetOfConsumeQueue) {
                    // It's safe to recover from this mapped file
                    // 该文件之前的 mappedFile 全部被构建索引了，全部有效
                    // 该文件内部可能一部分构建索引，也可能有一部分没有构建索引，所以要从该文件开始 recover
                    break;
                }
                index--;
            }
            // TODO: Discuss if we need to load more commit-log mapped files into memory.

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 从 FileFromOffset 位置处开始处理
            long processOffset = mappedFile.getFileFromOffset();
            // 已经处理的字节数
            long mappedFileOffset = 0;
            // 从 storeCheckpoint 文件中恢复
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            while (true) {
                /**
                 * 返回信息 DispatchRequest 中 true 表示 success , 校验消息成功，返回的 msgSize 为刚刚校验过的完整消息size
                 * msgSize = 0 也表示校验成功，意思是整个 byteBuffer 已经校验完毕，到了文件末尾了，没有消息了
                 * false 表示校验失败，msgSize 返回 -1 ， 如果 [BUG]read total count not equals msg total size ，msgSize返回 total size
                 *
                 * 这里是挨个校验 mappedFile 中的消息是否正确，需要校验消息体的 CRC
                 */
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                // true 表示还未构建索引
                boolean doDispatch = dispatchRequest.getCommitLogOffset() > maxPhyOffsetOfConsumeQueue;
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    // 增加有效消息的 offset
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;
                    // CommitLogDispatcherCalcBitMap
                    // CommitLogDispatcherBuildConsumeQueue
                    // CommitLogDispatcherBuildIndex
                    // CommitLogDispatcherCompaction
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    // isFileEnd = true 不触发 dispatch（因为这个 dispatchRequest 不包含任何信息）
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        // 最后一个文件 recover 完毕
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        // 继续校验下一个文件
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    // 遇到无效消息停止遍历 mappedFile,后续从无效消息处开始截断
                    break;
                }
            }
            // processOffset 现在就是 commitlog 中最大的有效 offset
            processOffset += mappedFileOffset;
            // conmfirmOffset 为 commitlog 最大有效的 offset
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > processOffset) {
                    log.error("confirmOffset {} is larger than processOffset {}, correct confirmOffset to processOffset", this.defaultMessageStore.getConfirmOffset(), processOffset);
                    this.defaultMessageStore.setConfirmOffset(processOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                // 从 processOffset 位置开始截断 consume queue
                // 因为 [processOffset, maxPhyOffsetOfConsumeQueue] 之间的消息是无效的
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }

            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     *
     * 返回信息 DispatchRequest 中 true 表示 success , 校验消息成功，返回的 msgSize 为刚刚校验过的完整消息size
     * msgSize = 0 也表示校验成功，意思是整个 byteBuffer 已经校验完毕，到了文件末尾了，没有消息了
     *
     * false 表示校验失败，msgSize 返回 -1 ， 如果 [BUG]read total count not equals msg total size ，msgSize返回total size
     *
     * 读取到的消息 size 是否和 commitlog 中指定的 totalSize 一致
     * 一致的话最起码说明格式是正确的，这里我们不校验内容，也就是只校验格式是否正确，不校验 CRC
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {
        try {
            if (byteBuffer.remaining() <= 4) {
                return new DispatchRequest(-1, false /* fail */);
            }
            // 1 TOTAL SIZE // 整个消息条目占用的总字节数
            int totalSize = byteBuffer.getInt();
            if (byteBuffer.remaining() < totalSize - 4) { // 这里 -4 是因为前面已经读取出了 4 个字节了（TOTAL SIZE）
                return new DispatchRequest(-1, false /* fail */);
            }

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt(); // 固定值 0xdaa320a7，用于快速校验文件是否为合法的RocketMQ CommitLog 文件
            switch (magicCode) {
                case MessageDecoder.MESSAGE_MAGIC_CODE:
                case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                    break;
                case BLANK_MAGIC_CODE: // // End of file empty MAGIC CODE cbd43194
                    // 校验到 mappedFile 的末尾，下次循环开始校验下一个 mappedFile
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            MessageVersion messageVersion = MessageVersion.valueOfMagicCode(magicCode);

            byte[] bytesContent = new byte[totalSize];
            // 3. 消息体的CRC32校验码，用于在恢复或读取时检测消息体数据是否损坏
            int bodyCRC = byteBuffer.getInt();
            // 4. 消息所属的消费队列ID
            int queueId = byteBuffer.getInt();
            // 5. 供应用程序使用的标志位，RocketMQ本身不处理
            int flag = byteBuffer.getInt();
            // 6. 消息在当前ConsumeQueue中的逻辑偏移量，可以理解为该队列中消息的序号, offset 均为全局概念
            // 在生产消息的时候如何得知 queueOffset ？ 注意这里不是字节为单位的 offset , 而是表示第几个消息索引（消息索引粒度）
            // topic-> queueId 维度中的第几个消息索引（全局）
            long queueOffset = byteBuffer.getLong();
            // 7. 消息在整个CommitLog文件中的起始物理偏移量
            long physicOffset = byteBuffer.getLong();
            // 8. 消息的系统标志，用于标识消息是否为压缩消息、事务消息（如Prepared、Commit、Rollback状态）等
            // see : org.apache.rocketmq.common.sysflag.MessageSysFlag
            int sysFlag = byteBuffer.getInt();
            // 9. 消息在生产者端被创建的时间戳
            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                // 10. ipv4:BornHost : 生产者的IP地址和端口号
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            // 11. 消息在Broker端被成功存储的时间戳
            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                // 12. ipv4: StoreHostAddress : 存储该消息的Broker的IP地址和端口号
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            // 13. 消息可以被某个消费组重新消费的次数
            int reconsumeTimes = byteBuffer.getInt();
            // 14. 事务消息相关字段。如果是事务Prepare消息，则记录其在CommitLog中的偏移量；否则为0
            long preparedTransactionOffset = byteBuffer.getLong();
            // 15. 消息体的长度（字节数）
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    // 16 . 消息的真实业务数据(消息体)
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        /**
                         * When the forceVerifyPropCRC = false,
                         * use original bodyCrc validation.
                         */
                        if (!this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                            // 校验消息体的 CRC
                            int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                            if (crc != bodyCRC) {
                                log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                                return new DispatchRequest(-1, false/* success */);
                            }
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            // 17. 消息所属主题名称的长度，限制了主题名最长不能超过255个字符
            int topicLen = messageVersion.getTopicLength(byteBuffer);
            // 18. 消息所属的主题名称
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            // 可以指定多个，后台会为每个 key 建立 index(indexFile)
            String keys = "";
            // 只能指定一个，功能都是一样的
            String uniqKey = null;
            // 19. 消息的自定义属性（如消息的TAG、KEY等）的总长度
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                // 20. 消息的自定义属性内容，以特定格式（如key1:value1\u0001key2:value2）存储（properties格式）
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                if (checkDupInfo) {
                    String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
                    if (null == dupInfo || dupInfo.split("_").length != 2) {
                        log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
                        return new DispatchRequest(-1, false);
                    }
                }

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (!Strings.isNullOrEmpty(tags)) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            if (checkCRC) {
                /**
                 * When the forceVerifyPropCRC = true, 默认为 false
                 * Crc verification needs to be performed on the entire message data (excluding the length reserved at the tail)
                 */
                if (this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                    int expectedCRC = -1;
                    if (propertiesMap != null) {
                        String crc32Str = propertiesMap.get(MessageConst.PROPERTY_CRC32);
                        if (crc32Str != null) {
                            expectedCRC = 0;
                            for (int i = crc32Str.length() - 1; i >= 0; i--) {
                                int num = crc32Str.charAt(i) - '0';
                                expectedCRC *= 10;
                                expectedCRC += num;
                            }
                        }
                    }
                    if (expectedCRC >= 0) {
                        ByteBuffer tmpBuffer = byteBuffer.duplicate();
                        tmpBuffer.position(tmpBuffer.position() - totalSize);
                        tmpBuffer.limit(tmpBuffer.position() + totalSize - CommitLog.CRC32_RESERVED_LEN);
                        int crc = UtilAll.crc32(tmpBuffer);
                        if (crc != expectedCRC) {
                            log.warn(
                                "CommitLog#checkAndDispatchMessage: failed to check message CRC, expected "
                                    + "CRC={}, actual CRC={}", bodyCRC, crc);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    } else {
                        log.warn(
                            "CommitLog#checkAndDispatchMessage: failed to check message CRC, not found CRC in properties");
                        return new DispatchRequest(-1, false/* success */);
                    }
                }
            }
            // 读取到的消息 size 是否和 commitlog 中指定的 totalSize 一致
            // 一致的话最起码说明格式是正确的，这里我们不校验内容，也就是只校验格式是否正确，不校验 CRC
            int readLength = MessageExtEncoder.calMsgLength(messageVersion, sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode, // 消息tag 的 hashcode
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );

            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);

            return dispatchRequest;
        } catch (Exception e) {
            log.error("checkMessageAndReturnSize failed, may can not dispatch", e);
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    // Fetch and compute the newest confirmOffset.
    // Even if it is just inited.
    // ConfirmOffset 表示存储在 commitlog 中的 offset , bytes that have been stored in commit log
    // commit 到 page cache 的 offset(transientStorePool开启的情况下)
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1
                    || !this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
                // First time it will compute the confirmOffset.
                if (this.confirmOffset < 0) {
                    setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                    log.info("Init the confirmOffset to {}.", this.confirmOffset);
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return this.defaultMessageStore.isSyncDiskFlush() ? getFlushedWhere() : getMaxOffset();
        }
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            // slave 的 ConfirmOffset 就是 maxOffset
            return getMaxOffset();
        }
    }
    // 在 recover 的时候,起始 confirmOffset：
    // controller 模式:已经被构建索引的最大消息 offset ,位于 offset 之前的消息全部被构建索引了
    // 非 controller 模式就是 lastValidMsgPhyOffset
    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }

        return -1;
    }

    /**
     * maxPhyOffsetOfConsumeQueue: 当前被索引的最大 commitlog offset
     * @throws RocksDBException only in rocksdb mode
     */
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        // recover by the minimum time stamp
        // checkCRCOnRecover = true , 在 recover 的时候需要校验每条消息的 CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        // duplicationEnable = false
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        // 获取所有 commitlog 文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            // 从最后一个文件开始向前查找
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                /**
                 * 首先读取文件内的第一个消息，如果 magicCode 不是 MESSAGE_MAGIC_CODE 返回 false,继续向前查找，这个消息依然包含在 recover 范围
                 * 取出消息的 storeTimestamp ，storeTimestamp = 0 返回 false
                 * 如果 storeTimestamp <= (StoreCheckpoint 中的 physicMsgTimestamp 与 logicsMsgTimestamp 之间的最小值) 返回 true
                 * 大于的情况返回 false
                 *
                 * 如果一个 mappedFile 第一个消息的 storeTimestamp 是有效的，那么之前的 mappedFile 肯定有效，就从当前 mappedFile 往后挨个 recover
                 * */
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 开始 recover 的起始 phyOffset
            long processOffset = mappedFile.getFileFromOffset();
            // 已经 recover 的 offset，遇到有效消息就累加
            long mappedFileOffset = 0;
            // 遇到有效消息就更新
            long lastValidMsgPhyOffset = processOffset;
            // 已经被构建索引的最大消息 offset
            // 位于 offset 之前的消息全部被构建索引了
            long lastConfirmValidMsgPhyOffset = processOffset;
            // abnormal recover require dispatching
            // 异常关闭情况下，永远是要 doDispatch 构建索引的
            // 正常关闭情况下，只要 msgOffset >= maxPhyOffsetOfConsumeQueue 才会 doDispatch 构建
            boolean doDispatch = true;
            while (true) {
                /**
                 * 返回信息 DispatchRequest 中 true 表示 success , 校验消息成功，返回的 msgSize 为刚刚校验过的完整消息size
                 * msgSize = 0 也表示校验成功，意思是整个 byteBuffer 已经校验完毕，到了文件末尾了，没有消息了
                 * false 表示校验失败，msgSize 返回 -1 ， 如果 [BUG]read total count not equals msg total size ，msgSize返回 total size
                 *
                 * 这里是挨个校验 mappedFile 中的消息是否正确，需要校验消息体的 CRC
                 */
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                // 有效消息
                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        // 遇到有效消息就更新
                        lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                        // 遇到有效消息就累加
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable() || this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                            if (dispatchRequest.getCommitLogOffset() + size <= this.defaultMessageStore.getCommitLog().getConfirmOffset()) {
                                // doDispatch 构建索引
                                this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                                // 已经被构建索引的最大消息 offset
                                lastConfirmValidMsgPhyOffset = dispatchRequest.getCommitLogOffset() + size;
                            }
                        } else {
                            this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                        index++;
                        // recover 到最后一个文件末尾则停止
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            // 继续向后 recover
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {

                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }

                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            try {
                // consume queue flush
                this.getMessageStore().getQueueStore().flush();
            } catch (StoreException e) {
                log.error("Failed to flush ConsumeQueueStore", e);
            }
            // commitlog 最后一个有效消息的 offset
            processOffset += mappedFileOffset;
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > lastConfirmValidMsgPhyOffset) {
                    log.error("confirmOffset {} is larger than lastConfirmValidMsgPhyOffset {}, correct confirmOffset to lastConfirmValidMsgPhyOffset", this.defaultMessageStore.getConfirmOffset(), lastConfirmValidMsgPhyOffset);
                    // 已经被构建索引的最大消息 offset
                    // 位于 offset 之前的消息全部被构建索引了
                    this.defaultMessageStore.setConfirmOffset(lastConfirmValidMsgPhyOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }

            // Clear ConsumeQueue redundant data
            // [processOffset, maxPhyOffsetOfConsumeQueue] 这段消息是无效的，所以要从 processOffset 处开始截断
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }

            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public void truncateDirtyFiles(long phyOffset) {
        // 如果 phyOffset 到 FlushedWhere 之间的 commitlog 已经 flush 到磁盘了
        // 那么就将下次 flush 的起点重新设置为 phyOffset ，后续会将原来 phyOffset 到 FlushedWhere 之间的无效 commitlog 覆盖
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }
        // 同样的道理如果 phyOffset 到 FlushedWhere 之间的 commitlog 已经到 page cache 了
        // 下次重新覆盖点这段内容就行
        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }
        // 将 phyOffset 之外的 commitlog 删除
        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            // ConfirmOffset 表示已经存储在 commitlog 中的有效位点
            this.setConfirmOffset(phyOffset);
        }
    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }
    /**
     * 首先读取文件内的第一个消息，如果 magicCode 不是 MESSAGE_MAGIC_CODE 返回 false,继续向前查找，这个消息依然包含在 recover 范围
     * 取出消息的 storeTimestamp ，storeTimestamp = 0 返回 false
     * 如果 storeTimestamp <= (StoreCheckpoint 中的 physicMsgTimestamp 与 logicsMsgTimestamp 之间的最小值) 返回 true
     * 大于的情况返回 false
     *
     * 如果一个 mappedFile 第一个消息的 storeTimestamp 是有效的，那么之前的 mappedFile 肯定有效，就从当前 mappedFile 往后挨个 recover
     * */
    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) throws RocksDBException {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            // 无效消息继续向前一个文件查找
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isEnableRocksDBStore()) {
            final long maxPhyOffsetInConsumeQueue = this.defaultMessageStore.getQueueStore().getMaxPhyOffsetInConsumeQueue();
            long phyOffset = byteBuffer.getLong(MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
            if (phyOffset <= maxPhyOffsetInConsumeQueue) {
                log.info("find check. beginPhyOffset: {}, maxPhyOffsetInConsumeQueue: {}", phyOffset, maxPhyOffsetInConsumeQueue);
                return true;
            }
        } else {
            int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength;
            long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
            if (0 == storeTimestamp) {
                return false;
            }
            // messageIndexEnable = true,messageIndexSafe = false
            if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
                // physicMsgTimestamp 与 logicsMsgTimestamp ,indexMsgTimestamp 之间的最小值
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            } else {
                // physicMsgTimestamp 与 logicsMsgTimestamp 之间的最小值
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            }
        }

        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10 &&
            putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }
        // Set the message body CRC (consider the most appropriate setting on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        if (enabledAppendPropCRC) {
            // delete crc32 properties if exist
            msg.deleteProperty(MessageConst.PROPERTY_CRC32);
        }
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && topic.length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(putMessageThreadLocal);
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(msg);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        topicQueueLock.lock(topicQueueKey);
        try {

            boolean needAssignOffset = true;
            if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                needAssignOffset = false;
            }
            if (needAssignOffset) {
                defaultMessageStore.assignOffset(msg);
            }

            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            putMessageLock.lock(); //spin or ReentrantLock, depending on store config
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    msg.setStoreTimestamp(beginLockTimestamp);
                }

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    // 关闭文件预读（默认开启）
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(msg, getMessageNum(msg));
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());
        // 因为我们可能配置的是同步刷盘的策略，所以需要等 page cache 中的消息 flush 到磁盘之后才能返回。
        // 另一方面是 HA 的需求，我们可能要求消息被复制到 n 个 slave 之后才能返回。
        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        topicQueueLock.lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch);

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(messageExtBatch, (short) putMessageContext.getBatchSize());
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    private boolean needHandleHA(MessageExt messageExt) {

        if (!messageExt.isWaitStoreMsgOK()) {
            /*
              No need to sync messages that special config to extra broker slaves.
              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
             */
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
        MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        // 等待消息 flush 到磁盘
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            // 等待消息同步到 slave
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, PutMessageResult putMessageResult,
        int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        // SlaveTimeout = 3000
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        // groupTransferService.putRequest
        // 超时也由 groupTransferService 判断
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset() && offset + size <= this.getMaxOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(pos, size);
            if (null != selectMappedBufferResult) {
                selectMappedBufferResult.setInCache(coldDataCheckService.isDataInPageCache(offset));
                return selectMappedBufferResult;
            }
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    protected short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        CQType cqType = getCqType(msgInner);

        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
            }
            return CommitRealTimeService.class.getSimpleName();
        }

        /**
         * commit 条件
         * 1. 如果 file 写满了，则立即无条件 commit
         * 2. 如果 transientStorePool 中的未commit的数据已经停留超过了 200ms 没有 commit,则立即commit
         * 3. 如果 transientStorePool 中的未commit的数据积累达到了 16k(4 pages),则立即commit
         *
         * */
        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 每隔 200ms 检查是否需要 commit transientStorePool 中的数据
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                // 4，如果 transientStorePool 中的未commit的数据积累达到了 16k(4 pages),则立即commit
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                // 200，如果 transientStorePool 中的未commit的数据已经停留超过了 200ms 没有 commit,则立即commit
                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    // TransientStorePool 中的未commit数据超过 200ms 就要 commit 到 page cache
                    commitDataLeastPages = 0;
                }

                try {
                    // commit CommittedWhere 所在的 mappedFile , writeBuffer 在 mappedFile 中
                    // writeBuffer 也是 1G ，其中的数据是与 mappedFile 一致的
                    // 每次是从 writeBuffer 的 COMMITTED_POSITION_UPDATER 位置开始到 writePos
                    // 将 writeBuffer 中的这段数据 commit 到 page cache 的对应位置
                    // commit 之后的数据仍然会停留在 writeBuffer 中，下次向 writeBuffer 写入继续从 writePos 开始
                    // 整个 mappedFile 文件中的数据全部写满，并且全部 commit 之后，returnBuffer
                    // writeBuffer 和 mappedFile 是一一对应的关系，大小一致都是 1G ， 里面的数据也是全部一样的
                    // mappedFile 写满了也全 commit 了，那自然 writeBuffer 也就没用了该归还了
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        CommitLog.this.flushManager.wakeUpFlush();
                    }
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    // 每隔 200ms 触发一次 TransientStorePool commit 检测
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }
    // FlushDiskType.ASYNC_FLUSH
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        /**
         * flush 条件
         * 1. 如果超过了 10s 未 flush 也就是脏页在 page cache 中停留时间超过 10s 的时间（时间维度）
         * 2. page cache 积累的未 flush 数据超过了 16K（4 pages）（空间维度）
         * 3. 只要文件写满了则立即flush
         * */
        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 默认 true : Whether schedule flush
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                // 500
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 4
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                // 1000 * 10
                // 如果超过了 10s 未 flush 也就是脏页在 page cache 中停留时间超过 10s 的时间
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    // 如果超过了 10s 未 flush 也就是脏页在 page cache 中停留时间超过 10s 的时间
                    // 0 表示只要有脏页就 flush,不管脏页究极有多少
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        // 等待 500ms, 每隔 500ms 触发 flush 条件检测
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // 只 flush FlushedWhere 所在的 mappedFile
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    // 最后一个被 flush 的 message store timestamp
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }
            // FlushRealTimeService stop
            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                // 只要有脏页就 flush
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
            }
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }
    public static class GroupCommitRequest {
        // 消息写入到 commitlog 的位置 ： writePos + writeBytes
        // 只要 flushPosition 超过了这里，就说明该消息已经被 flush 到磁盘了，通知 flushOKFuture complete with ok
        // 由 GroupCommitService（同步刷盘策略的flushservice） 进行通知
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        // deadLine 到达的时候，这里的 flushOKFuture 会 complete with timeout
        // 由 FlushDiskWatcher 负责完成这个动作(同步刷盘场景)
        // HA 场景超时由 groupTransferService 判断
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        // HA 策略，消息写入到 commitlog 的位置 ： writePos + writeBytes
        // 消息写入 commitlog 之后，根据 HA 的需求需要复制到 ackNums 个 slave
        // 当 ackNums 个 slave , ackoffset 超过 nextOffset 的时候说明已经复制到 slave 了，通知 flushOKFuture complete with ok
        // 由 GroupTransferService 负责通知
        // 超时也由 groupTransferService 判断
        private volatile int ackNums = 1;
        // timeout 时间戳，纳秒
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            // 消息写入到 commitlog 的位置 ： writePos + writeBytes
            // 只要 flushPosition 超过了这里，就说明该消息已经被 flush 到磁盘了，通知 flushOKFuture complete with ok
            // 由 GroupCommitService（同步刷盘策略的flushservice） 进行通知
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public int getAckNums() {
            return ackNums;
        }

        public long getDeadLine() {
            return deadLine;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    /**
     * GroupCommit Service
     * SYNC_FLUSH
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 1000 && !flushOK; i++) {
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        if (flushOK) {
                            break;
                        } else {
                            // When transientStorePoolEnable is true, the messages in writeBuffer may not be committed
                            // to pageCache very quickly, and flushOk here may almost be false, so we can sleep 1ms to
                            // wait for the messages to be committed to pageCache.
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }

                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            this.swapRequests();
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
            }
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class GroupCheckService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

        public boolean isAsyncRequestsFull() {
            return requestsWrite.size() > CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests() * 2;
        }

        public synchronized boolean putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
            boolean flag = this.requestsWrite.size() >
                CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
            if (flag) {
                log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(),
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
            }

            return flag;
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 1000; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                            if (flushOK) {
                                break;
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (Throwable ignored) {

                                }
                            }
                        }
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(1);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCheckService.class.getSimpleName();
            }
            return GroupCheckService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        private final int crc32ReservedLength;
        private final MessageStoreConfig messageStoreConfig;

        DefaultAppendMessageCallback(MessageStoreConfig messageStoreConfig) {
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            this.messageStoreConfig = messageStoreConfig;
            this.crc32ReservedLength = messageStoreConfig.isEnabledAppendPropCRC() ? CommitLog.CRC32_RESERVED_LEN : 0;
        }

        public AppendMessageResult handlePropertiesForLmqMsg(ByteBuffer preEncodeBuffer,
            final MessageExtBrokerInner msgInner) {
            if (msgInner.isEncodeCompleted()) {
                return null;
            }

            try {
                LmqDispatch.wrapLmqDispatch(defaultMessageStore, msgInner);
            } catch (ConsumeQueueException e) {
                if (e.getCause() instanceof RocksDBException) {
                    log.error("Failed to wrap multi-dispatch", e);
                    return new AppendMessageResult(AppendMessageStatus.ROCKSDB_ERROR);
                }
                log.error("Failed to wrap multi-dispatch", e);
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            boolean needAppendLastPropertySeparator = enabledAppendPropCRC && propertiesData != null && propertiesData.length > 0
                && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;

            final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            int msgLenWithoutProperties = preEncodeBuffer.getInt(0);

            int msgLen = msgLenWithoutProperties + 2 + propertiesLength;

            // Exceeds the maximum message
            if (msgLen > this.messageStoreConfig.getMaxMessageSize()) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", maxMessageSize: " + this.messageStoreConfig.getMaxMessageSize());
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Back filling total message length
            preEncodeBuffer.putInt(0, msgLen);
            // Modify position to msgLenWithoutProperties
            preEncodeBuffer.position(msgLenWithoutProperties);

            preEncodeBuffer.putShort((short) propertiesLength);

            if (propertiesLength > crc32ReservedLength) {
                preEncodeBuffer.put(propertiesData);
            }

            if (needAppendLastPropertySeparator) {
                preEncodeBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
            }
            // 18 CRC32
            preEncodeBuffer.position(preEncodeBuffer.position() + crc32ReservedLength);

            msgInner.setEncodeCompleted(true);

            return null;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            boolean isMultiDispatchMsg = messageStoreConfig.isEnableLmq() && msgInner.needDispatchLMQ();
            if (isMultiDispatchMsg) {
                AppendMessageResult appendMessageResult = handlePropertiesForLmqMsg(preEncodeBuffer, msgInner);
                if (appendMessageResult != null) {
                    return appendMessageResult;
                }
            }

            final int msgLen = preEncodeBuffer.getInt(0);
            preEncodeBuffer.position(0);
            preEncodeBuffer.limit(msgLen);

            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();

            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            Long queueOffset = msgInner.getQueueOffset();

            // this msg maybe an inner-batch msg.
            short messageNum = getMessageNum(msgInner);

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the consume queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                    maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                    msgIdSupplier, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4     // 1 TOTALSIZE
                + 4     // 2 MAGICCODE
                + 4     // 3 BODYCRC
                + 4     // 4 QUEUEID
                + 4;    // 5 FLAG
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            pos += 8;
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST
            pos += 4 + 8 + ipLen;
            // 11 STORETIMESTAMP refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());
            if (enabledAppendPropCRC) {
                // 18 CRC32
                int checkSize = msgLen - crc32ReservedLength;
                ByteBuffer tmpBuffer = preEncodeBuffer.duplicate();
                tmpBuffer.limit(tmpBuffer.position() + checkSize);
                int crc32 = UtilAll.crc32(tmpBuffer);   // UtilAll.crc32 function will change the position to limit of the buffer
                tmpBuffer.limit(tmpBuffer.position() + crc32ReservedLength);
                MessageDecoder.createCrc32(tmpBuffer, crc32);
            }

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            // Write messages to the queue buffer
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            msgInner.setEncodedBuff(null);

            if (isMultiDispatchMsg) {
                try {
                    LmqDispatch.updateLmqOffsets(defaultMessageStore, msgInner);
                } catch (ConsumeQueueException e) {
                    // Increase in-memory max offset of the queue should not fail.
                    return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                }
            }

            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            Long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());
                if (enabledAppendPropCRC) {
                    //append crc32
                    int checkSize = msgLen - crc32ReservedLength;
                    ByteBuffer tmpBuffer = messagesByteBuff.duplicate();
                    tmpBuffer.position(msgPos).limit(msgPos + checkSize);
                    int crc32 = UtilAll.crc32(tmpBuffer);
                    messagesByteBuff.position(msgPos + checkSize);
                    MessageDecoder.createCrc32(messagesByteBuff, crc32);
                }

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }

    class DefaultFlushManager implements FlushManager {
        // CommitLog.FlushRealTimeService
        private final FlushCommitLogService flushCommitLogService;

        //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
        private final FlushCommitLogService commitRealTimeService;

        public DefaultFlushManager() {
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                // SYNC_FLUSH 每隔 10ms，无条件flush
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                // ASYNC_FLUSH 每隔 500ms 且有条件刷新（超过10s,超过16K）
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }

            this.commitRealTimeService = new CommitLog.CommitRealTimeService();
        }

        @Override
        public void start() {
            this.flushCommitLogService.start();

            if (defaultMessageStore.isTransientStorePoolEnable()) {
                // 如果 TransientStorePoolEnable，那么就需要定时将 TransientStorePool 中的数据 commit 到 page cache
                this.commitRealTimeService.start();
            }
        }

        @Override
        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
            MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    // GroupCommitService ,see : org.apache.rocketmq.store.CommitLog.DefaultFlushManager.DefaultFlushManager
                    service.putRequest(request);
                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                    PutMessageStatus flushStatus = null;
                    try {
                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        //flushOK=false;
                    }
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    if (defaultMessageStore.getMessageStoreConfig().isWakeFlushWhenPutMessage()) {
                        flushCommitLogService.wakeup();
                    }
                } else {
                    if (defaultMessageStore.getMessageStoreConfig().isWakeCommitWhenPutMessage()) {
                        commitRealTimeService.wakeup();
                    }
                }
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    // SyncFlushTimeout = 1000 * 5
                    // 只要 commitlog flushPosition 超过了这里，就说明该消息已经被 flush 到磁盘了，通知 flushOKFuture complete with ok
                    // 由 GroupCommitService（同步刷盘策略的flushservice） 进行通知
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    // flushDiskWatcher 用于等到 request 过期的时候通知 flushOKFuture.complete(FLUSH_DISK_TIMEOUT)
                    flushDiskWatcher.add(request);
                    // GroupCommitService ,see : org.apache.rocketmq.store.CommitLog.DefaultFlushManager.DefaultFlushManager
                    service.putRequest(request);
                    // flush 操作虽然是异步执行，但是会把 flush 结果同步给 Future
                    // 返回 flushOKFuture
                    return request.future();
                } else {
                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    // false
                    if (defaultMessageStore.getMessageStoreConfig().isWakeFlushWhenPutMessage()) {
                        flushCommitLogService.wakeup();
                    }
                } else {
                    if (defaultMessageStore.getMessageStoreConfig().isWakeCommitWhenPutMessage()) {
                        commitRealTimeService.wakeup();
                    }
                }
                // wake up flush 线程，然后直接不管了，异步刷盘直接返回 completedFuture
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void wakeUpCommit() {
            // now wake up commit log thread.
            commitRealTimeService.wakeup();
        }

        @Override
        public void shutdown() {
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.shutdown();
            }

            this.flushCommitLogService.shutdown();
        }

    }

    public int getCommitLogSize() {
        return commitLogSize;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public FlushManager getFlushManager() {
        return flushManager;
    }

    private boolean isCloseReadAhead() {
        return !MixAll.isWindows() && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable();
    }
    /**
     * RIP-62: https://docs.google.com/document/d/1P8JPX5b9CwYMlaFi_UBxqwxFoIiOXWuAEjV4yWdfFUw/edit?tab=t.0
     *
     * 冷读控制的维度：冷读控制的维度主要为单消费者 和 全局两个维度。
     * 单消费者: 单位时间周期内，对某个消费者的读取的冷数据量进行累积，并对单个消费者的读取冷数据的行为进行控制。
     * 全局: 单位时间周期内，对整个系统中系统中所有的消费者读取的冷数据量进行统计，并对所有消费者的读取冷数据的行为进行控制。
     *
     * 冷读控制规则：对单个消费者和全局系统，都分别配置一个冷读数据量的最大值，在消费者在发起请求时，
     * 首先判断是否触发全局冷控(即当前所有的消费者读取的冷数据量超过了全局冷数据总阈值)，若被控制，则运行控制策略；
     * 否则再判断单个消费者当前读取的冷读数据量是否超过了当前单个消费者冷数据阈值，若被控制，则运行控制策略。
     *
     * 冷控后流程: 针对RocketMQ客户端的行为特点，客户端的类型主要分为Pull和Push类型的两种客户端。
     * 针对Pull客户端主要由用户自己编写控制逻辑的特点，在消费者触发到冷控机制后，系统对Pull的消费者实现了让其在服务端hold 一定的时间，
     * 读取一条数据返回的策略，从而降低消费速率；对于Push的客户端，在消费者触发到冷控机制后，系统直接返回SystemBusy，
     * 让该客户端在自己本地hold一定的时间再发起拉取消息请求，从而实现冷读控制。
     *
     * 流控算法 PID 公式： https://chat.deepseek.com/a/chat/s/c3ee068f-4ad1-43c8-bf36-1cd4d82ce917
     *
     * */
    public class ColdDataCheckService extends ServiceThread {
        private final SystemClock systemClock = new SystemClock();
        // key 表示一个 mappedFile , 用它的 FileName 也就是文件起始 offset 表示
        // value 为 byte[] pageCacheRst ，数组中表示对应文件中的每一个内存页是否仍然在内存中。 1 表示在，0 表示不在
        // 但这里的 value 是经过抽样后的 pageCacheTable，每隔 32 个页面记录一下页面驻留情况
        // see : scanFilesInPageCache 方法对 pageCacheTable 进行抽样检查，每隔 32 个页面记录一下页面驻留情况
        private final ConcurrentHashMap<String, byte[]> pageCacheMap = new ConcurrentHashMap<>();
        private int pageSize = -1;
        private int sampleSteps = 32;

        public ColdDataCheckService() {
            // 32
            sampleSteps = defaultMessageStore.getMessageStoreConfig().getSampleSteps();
            if (sampleSteps <= 0) {
                sampleSteps = 32;
            }
            // coldDataFlowControlEnable=false（默认）, 默认不会进行 cold data check
            // 在一些业务场景中，存在一些消费者读取Broker中存储的历史消息，而读取这部分的历史数据，
            // 往往需要消耗大量的磁盘IO，严重影响Broker的稳定性(包括 影响正常消息的写入、正常消息的读取)。
            // 为了保护系统能够正常运行，决定对系统的冷数据读取速度进行控制，从而保证系统的稳定性。
            initPageSize();
            // 利用 mincore 系统调用扫描 mappedFile 对应的 page cache 是否仍然在内存中
            // 数组表示文件的每一个文件页是否仍然在内存中
            scanFilesInPageCache();
        }

        @Override
        public String getServiceName() {
            return ColdDataCheckService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info("{} service started", this.getServiceName());
            while (!this.isStopped()) {
                try {
                    // windows 平台或者 coldDataFlowControlEnable=false（默认） 或者 coldDataScanEnable = false （默认）
                    // 不起作用
                    if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                        pageCacheMap.clear();
                        this.waitForRunning(180 * 1000);
                        continue;
                    } else {
                        // timerColdDataCheckIntervalMs = 60 * 1000
                        // 每隔 60s check 一下 cold data
                        this.waitForRunning(defaultMessageStore.getMessageStoreConfig().getTimerColdDataCheckIntervalMs());
                    }

                    if (pageSize < 0) {
                        initPageSize();
                    }

                    long beginClockTimestamp = this.systemClock.now();
                    // check cold data
                    scanFilesInPageCache();
                    long costTime = this.systemClock.now() - beginClockTimestamp;
                    log.info("[{}] scanFilesInPageCache-cost {} ms.", costTime > 30 * 1000 ? "NOTIFYME" : "OK", costTime);
                } catch (Throwable e) {
                    log.warn(this.getServiceName() + " service has e: {}", e);
                }
            }
            log.info("{} service end", this.getServiceName());
        }

        public boolean isDataInPageCache(final long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return true;
            }
            if (pageSize <= 0 || sampleSteps <= 0) {
                return true;
            }
            if (!defaultMessageStore.checkInColdAreaByCommitOffset(offset, getMaxOffset())) {
                return true;
            }
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                return false;
            }

            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
            if (null == mappedFile) {
                return true;
            }
            byte[] bytes = pageCacheMap.get(mappedFile.getFileName());
            if (null == bytes) {
                return true;
            }

            int pos = (int) (offset % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
            int realIndex = pos / pageSize / sampleSteps;
            return bytes.length - 1 >= realIndex && bytes[realIndex] != 0;
        }

        private void scanFilesInPageCache() {
            // windows 平台或者 coldDataFlowControlEnable=false（默认） 或者 coldDataScanEnable = false （默认）
            // 不起作用
            if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable() || pageSize <= 0) {
                return;
            }
            try {
                log.info("pageCacheMap key size: {}", pageCacheMap.size());
                // 清理 pageCacheMap 中已经过期的文件
                clearExpireMappedFile();
                // 遍历所有 mappedFile
                mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                    // 利用 mincore 系统调用扫描 mappedFile 对应的 page cache 是否仍然在内存中
                    // 数组表示文件的每一个文件页是否仍然在内存中
                    byte[] pageCacheTable = checkFileInPageCache(mappedFile);
                    // 32
                    if (sampleSteps > 1) {
                        // 对 pageCacheTable 进行抽样检查，每隔 32 个页面记录一下页面驻留情况
                        pageCacheTable = sampling(pageCacheTable, sampleSteps);
                    }
                    pageCacheMap.put(mappedFile.getFileName(), pageCacheTable);
                });
            } catch (Exception e) {
                log.error("scanFilesInPageCache exception", e);
            }
        }

        private void clearExpireMappedFile() {
            // 获取 commitlog 各个 mappedFile 的起始偏移 offset
            Set<String> currentFileSet = mappedFileQueue.getMappedFiles().stream().map(MappedFile::getFileName).collect(Collectors.toSet());
            pageCacheMap.forEach((key, value) -> {
                // key 表示一个 mappedFile , 用它的 FileName 也就是文件起始 offset 表示
                if (!currentFileSet.contains(key)) {
                    // 如果当前 mappedFileQueue 中不包含 key 所表示的 mappedFile,说明该文件已过期被删除了
                    // 这里就需要从 pageCacheMap 中清理过期的文件
                    pageCacheMap.remove(key);
                    log.info("clearExpireMappedFile fileName: {}, has been clear", key);
                }
            });
        }

        private byte[] sampling(byte[] pageCacheTable, int sampleStep) {
            // 32
            byte[] sample = new byte[(pageCacheTable.length + sampleStep - 1) / sampleStep];
            for (int i = 0, j = 0; i < pageCacheTable.length && j < sample.length; i += sampleStep) {
                // sample 中存放的是第一个页面，第32个页面，第64个页面 ........
                // 每隔 32 个页面记录一下页面驻留情况
                sample[j++] = pageCacheTable[i];
            }
            // 对 pageCacheTable 进行抽样检查，每隔 32 个页面记录一下页面驻留情况
            return sample;
        }

        private byte[] checkFileInPageCache(MappedFile mappedFile) {
            long fileSize = mappedFile.getFileSize();
            // 文件 page cache 的起始内存地址
            final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
            // 文件 page cache 占用的文件页个数
            // mincore 系统调用要求
            int pageNums = (int) (fileSize + this.pageSize - 1) / this.pageSize;
            // 用于表示每一页是否驻留在内存中
            byte[] pageCacheRst = new byte[pageNums];
            /**
             * mincore 是 Linux 系统提供的一个系统调用，用于查询一段内存区域中的页面是否当前驻留在物理内存中（即是否被缓存在 RAM 里，未被换出到 swap 或尚未加载
             *
             * int mincore(void *addr, size_t length, unsigned char *vec);
             * addr：要查询的内存起始地址。必须是页对齐的（通常是系统页大小的整数倍）
             * length：要查询的内存区域大小（字节数）
             * vec：输出缓冲区，用于存储每个页面的驻留状态。该数组长度至少为 (length + pagesize - 1) / pagesize 字节，
             * 每个字节对应一个页面，其最低有效位（LSB）为 1 表示该页面当前驻留在物理内存中，为 0 则表示不在内存中（可能被换出或从未被访问过）
             *
             * 返回值：成功时返回 0；失败时返回 -1，并设置 errno 指示错误原因
             * */
            int mincore = LibC.INSTANCE.mincore(new Pointer(address), new NativeLong(fileSize), pageCacheRst);
            if (mincore != 0) {
                log.error("checkFileInPageCache call the LibC.INSTANCE.mincore error, fileName: {}, fileSize: {}",
                    mappedFile.getFileName(), fileSize);
                for (int i = 0; i < pageNums; i++) {
                    // 系统调用失败，则默认全部页面在内存中
                    pageCacheRst[i] = 1;
                }
            }
            return pageCacheRst;
        }
        // coldDataFlowControlEnable = true 的时候才起作用
        private void initPageSize() {
            // coldDataFlowControlEnable = false, pageSize 初始 -1
            if (pageSize < 0 && defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                try {
                    // coldDataFlowControlEnable = true 的时候才起作用
                    if (!MixAll.isWindows()) {
                        // linux
                        pageSize = LibC.INSTANCE.getpagesize();
                    } else {
                        // windows 平台补齐作用
                        defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                        log.info("windows os, coldDataCheckEnable force setting to be false");
                    }
                    log.info("initPageSize pageSize: {}", pageSize);
                } catch (Exception e) {
                    defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                    log.error("initPageSize error, coldDataCheckEnable force setting to be false ", e);
                }
            }
        }

        /**
         * this result is not high accurate.
         */
        public boolean isMsgInColdArea(String group, String topic, int queueId, long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return false;
            }
            try {
                ConsumeQueueInterface consumeQueue = defaultMessageStore.findConsumeQueue(topic, queueId);
                if (null == consumeQueue) {
                    return false;
                }
                ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(offset, 1);
                if (null == bufferConsumeQueue || !bufferConsumeQueue.hasNext()) {
                    return false;
                }
                return defaultMessageStore.checkInColdAreaByCommitOffset(bufferConsumeQueue.next().getPos(), getMaxOffset());
            } catch (Exception e) {
                log.error("isMsgInColdArea group: {}, topic: {}, queueId: {}, offset: {}",
                    group, topic, queueId, offset, e);
            }
            return false;
        }
    }

    public void scanFileAndSetReadMode(int mode) {
        if (MixAll.isWindows()) {
            log.info("windows os stop scanFileAndSetReadMode");
            return;
        }
        try {
            log.info("scanFileAndSetReadMode mode: {}", mode);
            mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                setFileReadMode(mappedFile, mode);
            });
        } catch (Exception e) {
            log.error("scanFileAndSetReadMode exception", e);
        }
    }

    private int setFileReadMode(MappedFile mappedFile, int mode) {
        if (null == mappedFile) {
            log.error("setFileReadMode mappedFile is null");
            return -1;
        }
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int madvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (madvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), madvise, mode);
        }
        return madvise;
    }

    public ColdDataCheckService getColdDataCheckService() {
        return coldDataCheckService;
    }
}

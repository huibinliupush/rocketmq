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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.FileQueueLifeCycle;
import org.apache.rocketmq.store.queue.MultiDispatchUtils;
import org.apache.rocketmq.store.queue.QueueOffsetOperator;
import org.apache.rocketmq.store.queue.ReferredIterator;

public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * ConsumeQueue's store unit. Format:
     * <pre>
     * ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
     * │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
     * │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
     * ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
     * │                                     Store Unit                                    │
     * │                                                                                   │
     * </pre>
     * ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) = 20 Bytes
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;
    // 一个 consumer queue 对应多个 ConsumerQueue 文件（每个文件存储 30万 条消费索引）
    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    // 20 字节，一个消息索引
    private final ByteBuffer byteBufferIndex;
    // user.home/store/consumequeue
    private final String storePath;
    // 600万字节 ，单个索引 20 ， 可存储 300000 个
    private final int mappedFileSize;
    // consume queue 中消息索引中存储的最大 commitlog offset
    // 这个 offset 是最后一条消息在 commitlog 的末尾不是开头
    private long maxPhysicOffset = -1;

    /**
     * Minimum offset of the consume file queue that points to valid commit log record.
     * 字节为单位
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore messageStore) {
        // user.home/store/consumequeue
        this.storePath = storePath;
        // 600万字节 ，单个索引 20 ， 可存储 300000 个
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;

        this.topic = topic;
        this.queueId = queueId;
        // user.home/store/consumequeue/topic/queueId
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;
        // 一个 consumer queue 对应多个 ConsumerQueue 文件（每个文件存储 30万 条消费索引）
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        // 20 字节，一个消息索引
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        // 默认为 false , 用于存储 consume queue 的辅助信息，比如 consume filter bitsMap
        // see : CommitLogDispatcherCalcBitMap
        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()), // user.home/store/consumequeue_ext
                messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),// 48M
                messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    @Override
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }
    // 所谓 recover 其实就是计算出 consumequeue 中的 MaxPhysicOffset（挨个查找）
    // 另一个就是查找 consume queue中最后一个有效（offset >= 0 && size > 0）消息索引位置，其后的无效消息索引全部截断
    // 到这里可以看出来所谓recover consume queue，就是挨个遍历最后三个文件
    // 直到找出最后一个有效消息索引的 offset,用这个offset刷新 flushWhere,commitWhere
    // 以及截断 offset 之后的所有无效消息索引，顺便找到consumequeue 中的 MaxPhysicOffset
    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 始终只全量 recover 最后三个文件，在恢复精度和恢复效率之间做出的工程折衷。
            /**
             * 1.checkpoint之前的文件被认为是"安全的"
             * 正常退出时，checkpoint记录的时间戳之前的所有文件已经完整刷盘，数据是可靠的。因此不需要逐条验证，只需处理checkpoint之后可能存在的少量脏数据。
             *
             * 2.异步刷盘导致最后几个文件可能不完整
             * 由于异步刷盘机制，CommitLog和ConsumeQueue可能存在少量未及时刷盘的数据。这三个文件恰好覆盖了"刷盘时间点之后可能不一致"的范围。
             *
             * 3."3"是一个经验值
             * 源码注释和相关分析都指出，这只是一个工程经验值。三个ConsumeQueue文件大约对应17MB的数据量（单个文件约5.72MB），验证开销可控，
             * 同时又足以覆盖异步刷盘可能产生的所有不一致情况。
             *
             * 全部恢复:	扫描所有文件，重启时间过长，且没有必要（大部分文件已由checkpoint担保）
             * 只恢复最后一个:	如果异步延迟导致脏数据跨多个文件，可能漏检
             * 恢复三个:	覆盖异步刷盘延迟的常见范围，兼顾安全与效率
             * */
            int index = mappedFiles.size() - 3; // 计算从哪个文件开始 recover
            if (index < 0) {
                // 如果当前 consume queue 文件在 3 个以内，就从第一个开始 recover
                // 4 个 就从第二个，5个就从第3个，就是始终只全量 recover 最后三个文件
                index = 0;
            }

            int mappedFileSizeLogics = this.mappedFileSize;
            // 第一个 recover 的文件
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 当前开始处理的起始 offset , 每处理一个 mappedFile，这里就设置为它的 FileFromOffset
            long processOffset = mappedFile.getFileFromOffset();
            // 已经处理的过字节数
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    // 有效的消息索引
                    if (offset >= 0 && size > 0) {
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        // 不断地更新 consumequeue 中的 MaxPhysicOffset
                        this.setMaxPhysicOffset(offset + size);
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        // 无效数据，后续从mappedFileOffset开始开始截断
                        break;
                    }
                }
                // 到这里一个文件就 recover 完毕了，我们需要判断一下
                // 已经校验过的字节数mappedFileOffset是否和文件的size一致
                // 如果一致的话，说明这个文件是被写满的，如果是最后一个文件
                // 就recover完毕，如果不是就继续向后recover下一个文件
                // 说明整个文件都写满了
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 最后一个文件 recover 完毕
                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        // 获取下一个要 recover 的文件
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        // 已经处理的过字节数,没 recover 一个新的文件，这里就要清零
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    // 文件还没写满，说明就是最后一个文件了
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }
            // 到这里，最后三个文件已经全部 recover 了
            // processOffset 这里就是最后一条有效消息索引的全局 offset(消息的末尾位置而不是开始位置)
            processOffset += mappedFileOffset;
            // processOffset 为 recover 出来的有效消息 offset, 其后的数据全部截断
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    @Deprecated
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
            messageStore.getCommitLog(), BoundaryType.LOWER);
        return binarySearchInQueueByTime(mappedFile, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
            messageStore.getCommitLog(), boundaryType);
        return binarySearchInQueueByTime(mappedFile, timestamp, boundaryType);
    }

    private long binarySearchInQueueByTime(final MappedFile mappedFile, final long timestamp,
        BoundaryType boundaryType) {
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long minPhysicOffset = this.messageStore.getMinPhyOffset();
            int range = mappedFile.getFileSize();
            if (mappedFile.getWrotePosition() != 0 && mappedFile.getWrotePosition() != mappedFile.getFileSize()) {
                // mappedFile is the last one and is currently being written.
                range = mappedFile.getWrotePosition();
            }
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, range);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                int ceiling = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                int floor = low;
                high = ceiling;
                try {
                    // Handle the following corner cases first:
                    // 1. store time of (high) < timestamp
                    // 2. store time of (low) > timestamp
                    long storeTime;
                    long phyOffset;
                    int size;
                    // Handle case 1
                    byteBuffer.position(ceiling);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime < timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return (mappedFile.getFileFromOffset() + ceiling + CQ_STORE_UNIT_SIZE) / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return (mappedFile.getFileFromOffset() + ceiling) / CQ_STORE_UNIT_SIZE;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Handle case 2
                    byteBuffer.position(floor);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime > timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return mappedFile.getFileFromOffset() / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return 0;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Perform binary search
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        phyOffset = byteBuffer.getLong();
                        size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        storeTime = this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            log.warn("Failed to query store timestamp for commit log offset: {}", phyOffset);
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                        }
                    }

                    if (targetOffset != -1) {
                        // We just found ONE matched record. These next to it might also share the same store-timestamp.
                        offset = targetOffset;
                        switch (boundaryType) {
                            case LOWER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt - CQ_STORE_UNIT_SIZE;
                                    if (attempt < floor) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            case UPPER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt + CQ_STORE_UNIT_SIZE;
                                    if (attempt > ceiling) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                        .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    } else {
                        // Given timestamp does not have any message records. But we have a range enclosing the
                        // timestamp.
                        /*
                         * Consider the follow case: t2 has no consume queue entry and we are searching offset of
                         * t2 for lower and upper boundaries.
                         *  --------------------------
                         *   timestamp   Consume Queue
                         *       t1          1
                         *       t1          2
                         *       t1          3
                         *       t3          4
                         *       t3          5
                         *   --------------------------
                         * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
                         * contradictory at first sight, but it does make sense when performing range queries.
                         */
                        switch (boundaryType) {
                            case LOWER: {
                                offset = rightOffset;
                                break;
                            }

                            case UPPER: {
                                offset = leftOffset;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    }
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {

        int logicFileSize = this.mappedFileSize;
        // consumer queue 中的消息索引截断到 phyOffset（commitlog 中的 offset） 位置处
        this.setMaxPhysicOffset(phyOffset);
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        // 一个文件一个文件的截断，直到找到截断位点
        while (true) {
            // 从 conusme queue 的最近一个文件开始一个消息索引一个消息索引的检查
            // 只要消息索引在 commitlog 中的 offset 超过了 phyOffset ，就都是无效的需要截断
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                // 先清空 mappedFile 中的 position 信息，后续随着一个索引一个索引的检查
                // 会改变 position 的位置，position 之前的全部是有效的消息索引
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);
                // 从 consumer queue 的第一个字节开始检查
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    // 消息在 commit log 中的 offset
                    long offset = byteBuffer.getLong();
                    // 消息大小
                    int size = byteBuffer.getInt();
                    // 消息 tag
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        // 如果该 consumer queue 文件中的第一个消息 offset 就已经超过 phyOffset 了
                        // 那么这个文件里的消息索引就都是无效的，应该删除
                        if (offset >= phyOffset) {
                            shouldDeleteFile = true;
                            break;
                        } else {
                            // 消息的 offset 在有效范围内
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            // 更新 position 信息
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffset) {
                                // 如果 consumer queue 中的第二条及其以后的消息 offset 超过有效范围
                                // 那么就直接 return 就可以了，因为之前已经正确的更新过 mappedFile 相关 Position 信息了
                                // 这条消息就在 consumer queue 中忽略即可，不会计入 position 信息
                                return;
                            }
                            // 更新 mappedFile 相关 Position 信息(之前的全都是有效消息)
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                            // 如果整个 consumequeue 中都没有找到 offset  > 截断offset 的消息索引
                            // 那么之前的 mappedFiled 也不可能在出现了，直接返回
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }

                if (shouldDeleteFile) {
                    if (deleteFile) {
                        this.mappedFileQueue.deleteLastMappedFile();
                    } else {
                        this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                    }
                }

            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }
    // 如果 file 写满了，则立即无条件 flush
    // flushLeastPages = 0 , 则只要是 writePosition > flushPosition 就立即 flush
    // flushLeastPages > 0 ，则需要保证在 page cache 中积累的未 flush 的数据达到 flushLeastPages * 4K 才可能 flush
    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        // 只要 mappedFile 最后一个消息索引的 phyOffset < minOffset 那么就删除该文件，说明整个文件中的消息索引全部过期
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        // 现在过期的 consume queue 文件已经删除了，接下来需要根据 commitlog 的 minCommitLogOffset
        // 对 consume queue 的 minLogicOffset 进行修正，因为 commitlog 已经删了，这就导致了 consume queue 中的文件可能存在部分消息索引失效的情况
        // 修正后的 minLogicOffset 需要保证，该位置之后的消息所有全部有效，之前的消息索引全部无效
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     * 通过当前 commitlog 中最小的 minPhyOffset ，修正所有 consumerqueue 中的 minOffset（org.apache.rocketmq.store.ConsumeQueue#minLogicOffset）
     * @param minCommitLogOffset Minimum commit log offset
     *
     * 修正逻辑：
     * 快速路径：先查看最后一个 mappedFile 的最后一条消息索引 lastRecord 获取对应的  commitLogOffset
     * 如果最后一条消息索引都是无效的(commitLogOffset < minCommitLogOffset) , 那么整个 consume queue 都是无效的
     * minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition
     *
     * 如果最后一条消息索引是有效的那么就开始慢速路径查找
     * 慢速路径：获取整个 consume queue 第一个文件，在第一个文件中查找，因为这里是根据 commitlog 的最小 offset 进行修正
     * 所以 commitlog 最小的 offset 对应在 consumer queue 中的索引肯定存在于第一个 file 中
     * 首先我们从上一次的 minLogicOffset 位置处开始在第一个文件中查找，计算 minLogicOffset 在第一个文件中的内部 pos -> start
     * long start = this.minLogicOffset - mappedFile.getFileFromOffset()
     * start < 0 说明 minLogicOffset 在第一个文件之前的文件中,这不重要，直接从 0 开始在第一个文件中查找
     * start > mappedFile.getReadPosition() 说明 minLogicOffset 不在第一个文件，这个文件内的消息索引全都是无效的，
     * 本次不对这种情况进行修正，等待后续修正 。
     * Note the consume-queue deletion procedure ensures the last entry points to somewhere valid
     *
     * 下面就会从 start 位置处开始到第一个文件末尾（SelectMappedBufferResult）在这个范围内查找  minLogicOffset
     * 先快速检查 start 位置开始的第一个消息索引是否有效（commitLogOffset >= minCommitLogOffset），第一个有效的话后面就不用看了，全部有效
     *
     * 第一个消息无效的话，那么就开始在这段范围内进行二分查找
     * int low = 0;// minLogicOffset 位置开始的第一条消息索引
     * int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE; // mappedFile 最后一条索引的 offset
     * int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE; // 中间位置处的消息索引 offset
     * 通过先除以单元大小再乘回去，我们强制将 mid 对齐到最近的条目起始地址（向下取整）。例如：
     *      假设 CQ_STORE_UNIT_SIZE = 20。
     *      low = 0, high = 100，则 (0+100)/2 = 50。
 *          50 / 20 = 2（取整），2 * 20 = 40。
 *          最终 mid = 40，即第 2 条消息（索引从 0 开始）的起始位置。
     * mid.commitLogOffset > minCommitLogOffset  说明从 mid 到 high 之间的消息索引全部有效，那么就在 [low,mid]范围内查找 ——> high = mid
     * mid.commitLogOffset == minCommitLogOffset 说明 mid 位置正好就是  minLogicOffset
     * mid.commitLogOffset < minCommitLogOffset  说明从 low 到 mid 之间的消息索引全部有效，那么就在 [mid,high]范围内查找 ——> low = mid
     * 如果找了半天还是没有找到 minCommitLogOffset 的位置直到 high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE  停止二分查找
     * 那么就从 low 开始，挨个取出索引查看它的 offsetPy
     * for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE)
     * 如果找到一个 offsetPy >= minCommitLogOffset  那么  minLogicOffset =  mappedFile.getFileFromOffset() + start + i
     * 如果实在没有那就不做任何修改
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        // consume queue 是空的
        if (null == lastMappedFile) {
            return;
        }
        // 先检查最近的一条消息索引，如果无效的话，剩下的就不用看了
        SelectMappedBufferResult lastRecord = null;
        // 快速路径
        try {
            // mappedFile 的最大可读位置（write or confirm position）
            int maxReadablePosition = lastMappedFile.getReadPosition();
            // 读取最近一次写入的消息
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                // 最近写入的一条消息索引
                ByteBuffer buffer = lastRecord.getByteBuffer();
                long commitLogOffset = buffer.getLong();
                // lastRecord 是无效索引
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    // 将 consume queue 中的 minLogicOffset 指定为 maxReadablePosition 位置处
                    // maxReadablePosition 之前所有的消息索引都是无效的
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                        topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }
        // 在第一个文件中查找，因为这里是根据 commitlog 的最小 offset 进行修正
        // 所以 commitlog 最小的 offset 对应在 consumer queue 中的索引肯定存在于第一个 file 中
        // 下面就是从第一个 consumer queue file 中查找一个具体的 offset 位置，这个位置往后的消息索引全都是有效的
        // 首先应该从之前的 minLogicOffset 开始校验，随后开始在第一个 file 中进行二分查找
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            // 从上一次的 minLogicOffset 开始校验
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();// 计算 minLogicOffset 在 mappedFile 中的文件内偏移
            if (start < 0) {
                intact = false;
                // minLogicOffset 在第一个文件之前的文件中,这不重要，直接从 0 开始在第一个文件中查找
                start = 0;
            }
            // 如果文件内偏移pos 超过了本文件的最大可读位置（write or confirm position）
            // 说明 minLogicOffset 不在这个文件，这个文件内的消息索引全都是无效的
            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                    mappedFile.getFileName());
                return;
            }
            // minLogicOffset 位置处到文件末尾
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                    mappedFile.getFileName());
                return;
            }

            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }
                // minLogicOffset 位置处到文件末尾
                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                // 检验 consume queue 中从 minLogicOffset 位置开始的第一条消息索引是否有效
                long commitLogOffset = buffer.getLong();
                // 第一条消息有效那么后面的就不用看了,minLogicOffset继续保持不变
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                        commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;// minLogicOffset 位置开始的第一条消息索引
                // mappedFile 最后一条索引的 offset
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        break;
                    }
                    // 获取处于文件中间位置的索引 pos
                    /**
                     * 1. ConsumeQueue 文件是定长条目组成的数组。二分查找时，比较的对象是条目的第一个字段 commitLogOffset。
                     * 如果直接使用 (low + high) / 2 作为中间字节位置，这个位置很可能落在某条消息的中间（
                     * 例如在 commitLogOffset 之后的 4 字节处），此时读取 buffer.getLong() 就会得到错误的数据，导致查找失败。
                     *
                     * 2.通过先除以单元大小再乘回去，我们强制将 mid 对齐到最近的条目起始地址（向下取整）。例如：
                     *
                     *      假设 CQ_STORE_UNIT_SIZE = 20。
                     *
                     *      low = 0, high = 100，则 (0+100)/2 = 50。
                     *
                     *      50 / 20 = 2（取整），2 * 20 = 40。
                     *
                     *      最终 mid = 40，即第 2 条消息（索引从 0 开始）的起始位置。
                     *
                     * */
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    if (commitLogOffset > minCommitLogOffset) {
                        // 文件中间位置的消息索引有效，继续往文件前半部分查找
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        // 文件中间位置的消息索引恰好指向 minCommitLogOffset
                        // 那么这个位置就是整个 consuem queue 的 minOffset
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        // 文件中间位置的消息索引无效，继续往文件后半部分查找
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();

                    if (offsetPy >= minCommitLogOffset) {
                        // minLogicOffset 表示 consume queue 中，从这里开始往后全部都是有效的消息索引
                        // start 表示上一次的 minLogicOffset 在文件中的位置
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                            this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            // 消息 tag 的 hashcode
            long tagsCode = request.getTagsCode();
            // 默认 consumeQueueExt disable
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                // 由 CommitLogDispatcherCalcBitMap 计算 filter bit map
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                if (MultiDispatchUtils.checkMultiDispatchQueue(this.messageStore.getMessageStoreConfig(), request)) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        String[] queueOffsets = multiQueueOffset.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            if (StringUtils.contains(queueName, File.separator)) {
                continue;
            }
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);
        }
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
        int queueId) {
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                request.getTagsCode(),
                queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        // 当前 messageQueue 中最大的消息 index
        long queueOffset = queueOffsetOperator.getQueueOffset(topicQueueKey);
        msg.setQueueOffset(queueOffset);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg,
        short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        if (offset + size <= this.getMaxPhysicOffset()) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", this.getMaxPhysicOffset(), offset);
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        // CommitLogOffset
        this.byteBufferIndex.putLong(offset);
        // MsgSize
        this.byteBufferIndex.putInt(size);
        // 消息 tag hash code
        this.byteBufferIndex.putLong(tagsCode);
        // 注意这里的 cqOffset 不是字节为单位的 offset , 而是表示第几个消息索引（消息索引粒度）
        // topic -> queueId 的维度，第几个消息索引
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        // 获取 expectLogicOffset 所在的 mappedFile ，没有则创建（不预热）
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            // mappedFile 是第一个文件，但是还没有开始写内容，但指定的 cqOffset 不是 0
            // 那么就需要将 expectLogicOffset 之前的位置填充
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                // 那么就需要将 expectLogicOffset 之前的位置填充（默认使用 fileChannel）
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                // 获取当前 consume queue 的写入 offset (全局)
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            // consume queue 中消息索引中存储的最大 commitlog offset
            this.setMaxPhysicOffset(offset + size);
            boolean appendResult;
            // 使用 file channel 去写 consume queue
            if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
                appendResult = mappedFile.appendMessageUsingFileChannel(this.byteBufferIndex.array());
            } else {
                appendResult = mappedFile.appendMessage(this.byteBufferIndex.array());
            }
            return appendResult;
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            // 默认 true
            if (messageStore.getMessageStoreConfig().isPutConsumeQueueDataByFileChannel()) {
                mappedFile.appendMessageUsingFileChannel(byteBuffer.array());
            } else {
                mappedFile.appendMessage(byteBuffer.array());
            }

        }
    }

    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        // 从 consumerQueue 文件中获取从 offset 开始到 writePosition 之间的 buffer
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr);
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) {
        return iterateFrom(startIndex);
    }

    @Override
    public CqUnit get(long offset) {
        // 从 consumerQueue 文件中获取从 offset 开始到 writePosition 之间的 buffer
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        // 获取一个 unit
        return it.nextAndRelease();
    }

    @Override
    public Pair<CqUnit, Long> getCqUnitAndStoreTime(long index) {
        CqUnit cqUnit = get(index);
        Long messageStoreTime = this.messageStore.getQueueStore().getStoreTime(cqUnit);
        return new Pair<>(cqUnit, messageStoreTime);
    }

    @Override
    public Pair<CqUnit, Long> getEarliestUnitAndStoreTime() {
        CqUnit cqUnit = getEarliestUnit();
        Long messageStoreTime = this.messageStore.getQueueStore().getStoreTime(cqUnit);
        return new Pair<>(cqUnit, messageStoreTime);
    }

    @Override
    public CqUnit getEarliestUnit() {
        /**
         * here maybe should not return null
         */
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }

    private class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
        // 从 consumerQueue 文件中获取从 offset 开始到 writePosition 之间的 buffer
        private SelectMappedBufferResult sbr;
        // 所在 mappedFile 中的 position(文件内)
        private int relativePos = 0;

        public ConsumeQueueIterator(SelectMappedBufferResult sbr) {
            this.sbr = sbr;
            if (sbr != null && sbr.getByteBuffer() != null) {
                relativePos = sbr.getByteBuffer().position();
            }
        }

        @Override
        public boolean hasNext() {
            if (sbr == null || sbr.getByteBuffer() == null) {
                return false;
            }

            return sbr.getByteBuffer().hasRemaining();
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            // CqUnit 在 consumer queue 中的 offset(消费索引粒度)
            long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() - relativePos) / CQ_STORE_UNIT_SIZE;
            CqUnit cqUnit = new CqUnit(queueOffset,
                sbr.getByteBuffer().getLong(), // commit log 中的 offset
                sbr.getByteBuffer().getInt(),  // message size
                sbr.getByteBuffer().getLong());// message tag hashcode

            if (isExtAddr(cqUnit.getTagsCode())) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                boolean extRet = getExt(cqUnit.getTagsCode(), cqExtUnit);
                if (extRet) {
                    cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                    cqUnit.setCqExtUnit(cqExtUnit);
                } else {
                    // can't find ext content.Client will filter messages by tag also.
                    log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                        cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), getTopic());
                }
            }
            return cqUnit;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
            if (sbr != null) {
                sbr.release();
                sbr = null;
            }
        }

        @Override
        public CqUnit nextAndRelease() {
            try {
                return next();
            } finally {
                release();
            }
        }
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    @Override
    public void destroy() {
        this.setMaxPhysicOffset(-1);
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        long physicalOffsetFrom = from * CQ_STORE_UNIT_SIZE;
        long physicalOffsetTo = to * CQ_STORE_UNIT_SIZE;
        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long raw = 0;

        for (MappedFile mappedFile : mappedFiles) {
            int start = 0;
            int len = mappedFile.getFileSize();

            // calculate start and len for first segment and last segment to reduce scanning
            // first file segment
            if (mappedFile.getFileFromOffset() <= physicalOffsetFrom) {
                start = (int) (physicalOffsetFrom - mappedFile.getFileFromOffset());
                if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= physicalOffsetTo) {
                    // current mapped file covers search range completely.
                    len = (int) (physicalOffsetTo - physicalOffsetFrom);
                } else {
                    len = mappedFile.getFileSize() - start;
                }
            }

            // last file segment
            if (0 == start && mappedFile.getFileFromOffset() + mappedFile.getFileSize() > physicalOffsetTo) {
                len = (int) (physicalOffsetTo - mappedFile.getFileFromOffset());
            }

            // select partial data to scan
            SelectMappedBufferResult slice = mappedFile.selectMappedBuffer(start, len);
            if (null != slice) {
                try {
                    ByteBuffer buffer = slice.getByteBuffer();
                    int current = 0;
                    while (current < len) {
                        // skip physicalOffset and message length fields.
                        buffer.position(current + MSG_TAG_OFFSET_INDEX);
                        long tagCode = buffer.getLong();
                        ConsumeQueueExt.CqExtUnit ext = null;
                        if (isExtWriteEnable()) {
                            ext = consumeQueueExt.get(tagCode);
                            tagCode = ext.getTagsCode();
                        }
                        if (filter.isMatchedByConsumeQueue(tagCode, ext)) {
                            match++;
                        }
                        raw++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (raw >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (match > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
                            sample = true;
                            break;
                        }
                    }
                } finally {
                    slice.release();
                }
            }
            // we have scanned enough entries, now is the time to return an educated guess.
            if (sample) {
                break;
            }
        }

        long result = match;
        if (sample) {
            if (0 == raw) {
                log.error("[BUG]. Raw should NOT be 0");
                return 0;
            }
            result = (long) (match * (to - from) * 1.0 / raw);
        }
        log.debug("Result={}, raw={}, match={}, sample={}", result, raw, match, sample);
        return result;
    }
}

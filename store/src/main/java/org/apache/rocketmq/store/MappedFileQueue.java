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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

public class MappedFileQueue implements Swappable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    // user.home/stpre/commitlog
    // user.home/store/consumequeue/topic/queueId
    // user.home/stpre/timerlog
    protected final String storePath;

    protected final int mappedFileSize;

    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();
    // consume queue 这里为 null 因为是后台 reput 线程构建所以不需要异步创建，不用考虑文件的创建对实时性的影响
    // commit log 会使用 allocateMappedFileService， 因为消息是实时写入，不能受到文件创建开销的影响
    protected final AllocateMappedFileService allocateMappedFileService;
    // 全局 flushwhere , mappedFile 中有一个 flushPosition 指的是文件内部position(局部)
    protected long flushedWhere = 0;
    // 全局
    protected long committedWhere = 0;
    // 最后一个被 flush 的 message store timestamp
    protected volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        // user.home/stpre/commitlog
        // user.home/store/consumequeue/topic/queueId
        // user.home/stpre/timerlog
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        // timerlog , consume queue 这里为 null 因为是后台 reput 线程构建所以不需要异步创建，不用考虑文件的创建对实时性的影响
        // commit log 会使用 allocateMappedFileService， 因为消息是实时写入，不能受到文件创建开销的影响
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        if (!mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    // the adjacent mappedFile's offset don't match
                    // 相邻的两个 mappedFile 日志不连续
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog,
        BoundaryType boundaryType) {
        Object[] mfs = copyMappedFiles(0);
        if (null == mfs) {
            return null;
        }

        /*
         * Make sure each mapped file in consume queue has accurate start and stop time in accordance with commit log
         * mapped files. Note last modified time from file system is not reliable.
         */
        for (int i = mfs.length - 1; i >= 0; i--) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            // Figure out the earliest message store time in the consume queue mapped file.
            if (mappedFile.getStartTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStartTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
            // Figure out the latest message store time in the consume queue mapped file.
            if (i < mfs.length - 1 && mappedFile.getStopTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(mappedFileSize - ConsumeQueue.CQ_STORE_UNIT_SIZE, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStopTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
        }

        switch (boundaryType) {
            case LOWER: {
                for (int i = 0; i < mfs.length; i++) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (i < mfs.length - 1) {
                        long stopTimestamp = mappedFile.getStopTimestamp();
                        if (stopTimestamp >= timestamp) {
                            return mappedFile;
                        }
                    }

                    // Just return the latest one.
                    if (i == mfs.length - 1) {
                        return mappedFile;
                    }
                }
            }
            case UPPER: {
                for (int i = mfs.length - 1; i >= 0; i--) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (mappedFile.getStartTimestamp() <= timestamp) {
                        return mappedFile;
                    }
                }
            }

            default: {
                log.warn("Unknown boundary type");
                break;
            }
        }
        return null;
    }

    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    protected Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                // 如果 offset 正好处于一个 mappedFile 中间，那么就将该 mappedFile 相关 position 设置到 offset 处
                // 由于 offset 是 commitlog 全局的一个偏移，单个 mappedFile 中的 position 是文件内的相对偏移
                // 所以要将绝对偏移 offset 通过 % this.mappedFileSize 转换为相对偏移
                if (offset >= file.getFileFromOffset()) {
                    // file 中的 position 指的是文件内部的相对位置
                    // offset 指的是全局绝对位置
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // 如果整个文件都在 offset 之外，那么整个文件都是无效的，删除
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        // 将文件从 mappedFiles 中删除
        this.deleteExpiredFile(willRemoveFiles);
    }
    // 只有彻底 destroy 成功的 file 才会被加入到 files 中
    // 不包含 refCount > 0 无法删除的情况（这些 file 让其一直停留在 mappedFiles 中等下一次删除）
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }


    public boolean load() {
        // user.home/stpre/commitlog
        // storePath/consumerqueues/topic/queueid
        // user.home/stpre/timerlog
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // ascending order 先按照fileName进行排序offset从小到大
        files.sort(Comparator.comparing(File::getName));

        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.isDirectory()) {
                continue;
            }

            if (file.length() == 0 && i == files.size() - 1) {
                boolean ok = file.delete();
                log.warn("{} size is 0, auto delete. is_ok: {}", file, ok);
                continue;
            }

            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);
                // 后续 recover 会重新计算，重新设置
                // org.apache.rocketmq.store.ConsumeQueue.recover
                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.getFlushedWhere();
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 如果没有合适的 commitlog 文件来存放 startOffset 之后的数据，那么就新创建一个 mappedFile
        // 这里的 createOffset 就是新 mappedFile 文件的起始 offset(全局)
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            // 即将要创建的 mappedFile 起始 offset
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        // 文件写满了
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            // 对于 consume queue 来说只创建 nextFilePath 不会创建 nextNextFilePath
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    public boolean isEmptyOrCurrentFileFull() {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast == null) {
            return true;
        }
        if (mappedFileLast.isFull()) {
            return true;
        }
        return false;
    }

    public boolean shouldRoll(final int msgSize) {
        if (isEmptyOrCurrentFileFull()) {
            return true;
        }
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast.getWrotePosition() + msgSize > mappedFileLast.getFileSize()) {
            return true;
        }
        return false;
    }

    public MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        // consume queue 不使用 allocateMappedFileService
        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                // 如果不使用 allocateMappedFileService 的话，这里直接创建，不进行文件预热（只创建 nextFilePath）
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());
        List<MappedFile> toRemoves = new ArrayList<>();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                toRemoves.add(mappedFileLast);
            }
        }

        if (!toRemoves.isEmpty()) {
            this.mappedFiles.removeAll(toRemoves);
        }

        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 初始加载之前的 commitlog 文件时 ReadPosition 设置为 mappedFileSize
            // org.apache.rocketmq.store.MappedFileQueue.doLoad
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - getCommittedWhere();
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - this.getFlushedWhere();
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }
    /**
     *  expiredTime = 72 小时
     *  deleteFilesInterval = deleteCommitLogFilesInterval = 100
     *  intervalForcibly = destroyMapedFileIntervalForcibly = 1000 * 120
     *  cleanImmediately = cleanAtOnce
     *  deleteFileBatchMax = 10
     * */
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax) {
        // mappedFiles copy
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;
        // 只有一个文件的话就不清理
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        // 存储被 destroy 的 mappedFile
        List<MappedFile> files = new ArrayList<>();
        int skipFileNum = 0;
        if (null != mfs) {
            //do check before deleting
            // 检查相邻的两个 mappedFile 日志是否连续， 比较两者的 fileFromOffset 差值是否正好是一个 mappedFiledSize
            checkSelf();
            // 遍历 commitlog 所有的 mappedFile
            for (int i = 0; i < mfsLength; i++) {
                // 从最老的commitlog开始清理
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 最近的一次修改时间 + 72 小时
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                // 如果 72 小时之内没有任何写入操作 或者 cleanImmediately（commitlog或者consumequeue 所在磁盘分区使用率超过了 85% ） 则最最老的文件进行无条件 destroy
                // 磁盘使用率达到75%则对超过72小时的文件进行删除
                // 超过85%则从最老的文件开始直接删除（不管72小时的限制
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (skipFileNum > 0) {
                        log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
                    }
                    // 返回值 true 表示文件已经销毁，mappedByteBuffer(unmap),fileChannel.close,file.delete
                    // 返回 false, 表示第一次 shutdown 的时候 refCount > 0 ,或者距离第一次 destroyMapedFileIntervalForcibly(120s) 之内即使 refCount > 0
                    // 但超过 120s 就会强制 destroy
                    if (mappedFile.destroy(intervalForcibly)) {
                        // 已经被 destroy 的 mappedFile 加入到 file 集合中
                        files.add(mappedFile);
                        deleteCount++;
                        // 如果达到了每次批量删除文件的数目（10）
                        // 为了防止一次定时任务占用过长时间，单次清理任务最多只处理 10个文件
                        if (files.size() >= deleteFileBatchMax) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                // 每次 destroy 一个 mappedFile, 睡眠 100ms
                                // 如果最后一个 mappedFile destroy 则不需要在这里睡眠
                                // 避免因连续、密集的磁盘 IO 操作影响消息的读写性能
                                // 删除文件（尤其是通过 MappedFile.destroy 释放内存映射和物理文件）是一个相对耗时的 IO 操作。
                                // 如果一次性连续删除几十上百个文件而不加停顿，可能会瞬间占满磁盘 IO 资源，
                                // 导致 Broker 在该时刻处理消息写入（putMessage）或拉取（pullMessage）的延迟显著增加。
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        // 表示第一次 shutdown 的时候 refCount > 0 或者距离第一次shutdown的时间在 destroyMapedFileIntervalForcibly(120s) 之内即使 refCount > 0
                        // 如果删除某个文件时失败（例如文件正在被强制占用），则立即跳出循环，停止本次后续文件的删除尝试。
                        break;
                    }
                } else {
                    skipFileNum++;
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        // 从容器 mappedFiles 中清楚已经被 destroy 的 files
        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                // 从第一个 mappedFile 开始
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 取出文件中最后一个消息索引
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    // 最后一个消息索引记录的消息在 commitlog 中的 offset
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    // 最后一个消息索引的 phyOffset < commitlog 的 minOffset 说明是无效的，要 destroy
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                // commitlog 这里是 120s 的 intervalForcibly
                // consume queue 这里是 60s 的 intervalForcibly
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        // 只有彻底 destroy 成功的 file 才会被加入到 files 中
        // 不包含 refCount > 0 无法删除的情况（这些 file 让其一直停留在 mappedFiles 中等下一次删除）
        deleteExpiredFile(files);

        return deleteCount;
    }
    // offset : commitlog 最小 offsetPy
    // checkOffset: timer log 文件中最后一个 unit 的 offset（所有mappedFile中最后一个unit offset 都是一样的）
    // 因为 timerlog 中存储的都是固定长度的 TimerLog.UNIT_SIZE
    // 挨个遍历 timer log 的 mappedFile
    // 如果timerlog mappedFile中最后一个延时消息的 offsetPy 小于 commitlog 最小 offsetPy
    // 说明整个 timerlog mappedFile 过期，需要清理
    public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        // 获取 timer log 的所有 mappedFiles
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                // 挨个遍历 timer log 的 mappedFile
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取该 mappedFile 中最后一个 unit
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();//size
                        result.getByteBuffer().getLong(); //prev pos
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) { // unit 无损坏
                            result.getByteBuffer().position(position + MixAll.UNIT_PRE_SIZE_FOR_MSG);
                            // timerlog 中最后一个延时消息的 offsetPy 小于 commitlog 最小 offsetPy
                            // 说明整个 timerlog mappedFile 过期，需要清理
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset "
                                    + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(),
                                checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }
                // 销毁 timerlog mappedFile
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        // 从 mappedFiles 集合中删除
        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 只 flush FlushedWhere 所在的 mappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.getFlushedWhere(), this.getFlushedWhere() == 0);
        if (mappedFile != null) {
            // store timestamp of the last message.
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // 如果 file 写满了，则立即无条件 flush
            // flushLeastPages = 0 , 则只要是 writePosition > flushPosition 就立即 flush
            // flushLeastPages > 0 ，则需要保证在 page cache 中积累的未 flush 的数据达到 flushLeastPages * 4K 才可能 flush
            // offset 为新的 flushPosition(file局部位置)
            int offset = mappedFile.flush(flushLeastPages);
            // 获取全局 flushwhere
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.getFlushedWhere();
            this.setFlushedWhere(where);
            if (0 == flushLeastPages) {
                this.setStoreTimestamp(tmpTimeStamp);
            }
        }

        return result;
    }

    public synchronized boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 查询 CommittedWhere 所在的 mappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.getCommittedWhere(), this.getCommittedWhere() == 0);
        if (mappedFile != null) {
            // 返回 commitPosition(局部),writeBuffer在 mappedFile 中
            int offset = mappedFile.commit(commitLeastPages);
            // 全局，新的CommittedWhere
            long where = mappedFile.getFileFromOffset() + offset;
            // result = false means some data committed
            result = where == this.getCommittedWhere();
            // 更新 CommittedWhere
            this.setCommittedWhere(where);
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    // 每个 commitlog 文件是 1G，现在根据全局的 offset 就能定位到该 offset 所在 commitlog 文件 index
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.setFlushedWhere(0);

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs
                    && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
                continue;
            }
        }
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    public Object[] snapshot() {
        // return a safe copy
        return this.mappedFiles.toArray();
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public Stream<MappedFile> reversedStream() {
        return Lists.reverse(this.mappedFiles).stream();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }

    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }

    public String getStorePath() {
        return storePath;
    }

    public List<MappedFile> range(final long from, final long to) {
        Object[] mfs = copyMappedFiles(0);
        if (null == mfs) {
            return new ArrayList<>();
        }

        List<MappedFile> result = new ArrayList<>();
        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() <= from) {
                continue;
            }

            if (to <= mappedFile.getFileFromOffset()) {
                break;
            }
            result.add(mappedFile);
        }

        return result;
    }
}

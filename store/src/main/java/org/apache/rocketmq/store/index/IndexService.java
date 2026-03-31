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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    // 5000000
    private final int hashSlotNum;
    // 5000000 * 4
    private final int indexNum;
    // user.home/store/index
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        // 5000000
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 5000000 * 4
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // user.home/store/index
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load(final boolean lastExitOK) {
        // storePath/index 下的文件
        File dir = new File(this.storePath);
        // 获取 storePath/index 目录下的所有 indexFile
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 按照日期排序
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // load indexHeader
                    f.load();

                    if (!lastExitOK) {
                        // 异常关闭情况下一切以 storeCheckPoint 为准
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            // 无效 indexFile
                            f.destroy(0);
                            continue;
                        }
                    }

                    LOGGER.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    LOGGER.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    LOGGER.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public long getTotalSize() {
        if (indexFileList.isEmpty()) {
            return 0;
        }

        return (long) indexFileList.get(0).getFileSize() * indexFileList.size();
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }
            // 取出第一个 indexFile
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            // endPhyOffset < minCommitlogOffset 说明整个 indexFile 都是无效的
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            // 遍历所有 indexFile ，查看其 indexHeader 中的 EndPhyOffset
            // 如果 EndPhyOffset < minCommitlogOffset ，说明整个 indexFile 都是无效的，将其 destroy
            List<IndexFile> fileList = new ArrayList<>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }
            // destroy 无效的 indexFiles
            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    // 只有彻底 destroy 成功的 file 才会从 indexFileList 中 remove
                    // destroyed = false : refCount > 0 无法删除的情况（这些 file 让其一直停留在 indexFileList 中等下一次删除）
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        LOGGER.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        // 每批最多获取 64 个消息
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        List<Long> phyOffsets = new ArrayList<>(maxNum);
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    // 从最后一个 indexFile 开始查找
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }
                    // 查询时间范围 [begin, end] 与 indexFile header 中的时间范围有重叠
                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        // 查询完毕，下一个 indexFile 不可能包含时间范围 [begin,end] 的消息
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    public void buildIndex(DispatchRequest req) {
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            // 初始为前一个 indexFile 的 EndPhyOffset，后续会改变
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            // 已经构建过 index 了
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }
            // 消息 UniqKey 每个消息只能指定一个
            if (req.getUniqKey() != null) {
                // 添加索引修正 slotvalue, indexHeader
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }
            // 消息 key 可以指定多个，会为每个 key 建立索引
            if (keys != null && keys.length() > 0) {
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        // 为每一个消息key 建立索引
                        // 添加索引修正 slotvalue, indexHeader
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            LOGGER.error("build index error, stop building index");
        }
    }

    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            LOGGER.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");
            // 最多重试三次
            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            try {
                LOGGER.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getRunningFlags().makeIndexFileError();
            LOGGER.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    // 目前 IndexFile 中消息在 CommitLog 中的最大物理偏移
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    // 目前 IndexFile 中消息的最大存储时间
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }
        // 当前没有任何 indexFile 或者最后一个 indexFile 已经写满了
        if (indexFile == null) {
            try {
                // user.home/store/index/创建indexFile时候的时间戳（年月日时分秒毫秒）
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 创建 indexFile ，初始化 indexHeader
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                LOGGER.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
            // 每当新创建一个 indexFile 时候就会启动一个 flushThread 去 flush 前一个 indexFile
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;

                Thread flushThread = new Thread(new AbstractBrokerRunnable(defaultMessageStore.getBrokerConfig()) {
                    @Override
                    public void run0() {
                        IndexService.this.flush(flushThisFile);
                    }// 更新 indexHeader , flush index file
                }, "FlushIndexFileThread");
                /**
                 * 当一个 Daemon 线程的 run() 方法执行完毕（无论是正常结束还是抛出异常），该线程的主体执行逻辑就结束了，线程会进入终止状态，并退出。这一点与普通用户线程（User Thread）没有区别
                 * Daemon 线程的特殊性在于它对 JVM 的“守护”性质：
                 *
                 *  JVM 退出条件：JVM 会在所有非 Daemon 线程（用户线程）都结束后退出。
                 *
                 *  关键点：如果 JVM 退出了，Daemon 线程会直接被强制终止，即使它还没有执行完。
                 *
                 *  对比：如果是一个普通用户线程在执行完任务后正常退出，那只是线程结束；如果所有用户线程都结束了，JVM 就会退出，此时无论 Daemon 线程是否还在运行，都会被立即终止。
                 *
                 * 一个 Daemon 线程如果执行完了自己的代码，它就会像普通线程一样退出。但如果它还没执行完，而其他所有用户线程都已经结束了，JVM 会直接退出，此时它也会被“强行”终止（这种不算执行完后的正常退出）
                 *
                 * */
                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f) {
            return;
        }

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }
        // 更新 indexHeader , flush index file
        f.flush();

        if (indexMsgTimestamp > 0) { // 待 flush 的 indexFile 写满了才会更新 StoreCheckpoint
            // 最后一条 indexed 消息的 EndTimestamp
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {
        // 背后并没有后台线程，只是一个普通的 service
        // 提供对 indexFile 的相关操作
    }

    public void shutdown() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                try {
                    f.shutdown();
                } catch (Exception e) {
                    LOGGER.error("shutdown " + f.getFileName() + " exception", e);
                }
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("shutdown exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }
}

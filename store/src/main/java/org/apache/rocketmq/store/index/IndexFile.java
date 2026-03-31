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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    /**
     * Each index's store unit. Format:
     * <pre>
     * ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
     * │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
     * │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
     * ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
     * │                                 Index Store Unit                              │
     * │                                                                               │
     * </pre>
     * Each index's store unit. Size:
     * Key HashCode(4) + Physical Offset(8) + Time Diff(4) + Next Index Pos(4) = 20 Bytes
     */
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    // 500 万
    private final int hashSlotNum;
    // 2000 万
    private final int indexNum;
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;
    /**
     * fileName： user.home/store/index/创建indexFile时候的时间戳（年月日时分秒毫秒）
     * hashSlotNum：500万哈希槽
     * indexNum：2000万消息索引
     * endPhyOffset：前一个 indexFile 中存储的索引消息在 CommitLog 中的最大物理偏移
     * endTimestamp：前一个 indexFile 中存储的索引消息的最大存储时间
     * 如果当前还没有文件，则均为0，如果最后一个文件写满了，就是最后一个文件 end 相关属性
     * */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 40 + 500万 * 4 + 2000万*20 = 400M
        this.fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        // 初始化 indexHeader
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            // 初始为上一个 indexFile 的 endPhyOffset
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            // 初始为上一个 indexFile 的 endTimestamp
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        // refCount + 1
        if (this.mappedFile.hold()) {
            // 更新 indexHeader
            this.indexHeader.updateByteBuffer();
            // flush index file
            this.mappedByteBuffer.force();
            // refCount - 1
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        // IndexFile 中目前保存的消息索引条数已经达到指定的 indexNum = 200 万
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }
    // 添加索引修正 slotvalue, indexHeader
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 当前 indexFile 存储的消息索引数没有超过 indexNum（200万）
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // key 的 hash code
            int keyHash = indexKeyHashMethod(key);
            // 落在哪个哈希槽中
            int slotPos = keyHash % this.hashSlotNum;
            // 定位哈希槽的位置，每个哈希槽 4 字节
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 头插法
                // slotValue 存储的是当前槽内最新一个有效索引的位置 count(不包含空的尾结点)
                // indexCount 包含空的尾结点在内，空的尾结点用于指示该索引时槽内的最后一个索引
                // 该尾结点是所有槽内最后一个索引都会指向的节点（整个 indexFile就一个）
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // invalidIndex = 0 表示指向空的尾结点的位置，所有槽内最后一个索引均指向这个空的尾结点（整个 indexFile 只有一个）
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 秒级，这样可以用一个 int 存储
                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    // 整个 indexFiles 第一个消息索引的 timeDiff = 0
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                // 跳过 indexHeader , 500万的哈希槽
                // 跳过前面的索引
                // 这里采用头插法，因为 indexHeader.getIndexCount 的初始为 1 ，就是留有一个空的 20字节 index 索引作为尾节点（否则 indexCount 就是 0 了）
                // n->n-1 ... 4->3->2->1->0(尾结点)
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 消息 key 的 hashcode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // commitlog offset
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 初始 indexHeader.getBeginTimestamp = 0 那么 timeDiff = 0
                // 后续为 storeTimestamp - this.indexHeader.getBeginTimestamp()
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // Next Index Pos（指向上一次添加的索引位置）IndexCount
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 更新哈希槽的值为当前 IndexCount（索引未 put 之前的 indexCount）
                // slotValue 变化为 0 ，1，2，3，4..... n
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                // 添加第一个消息索引的时候更新 BeginPhyOffset，BeginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                // slotValue 初始为 0 ，表示未使用，后面保存的是当前槽内最后一个消息索引的前一个索引位置
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // 定位 slot 位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // 存储的是 slot 中最近的一个消息索引位置（index count）
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if 条件表示 slot 中是空的，只有一个尾结点，不包含任何消息 index
                // 空尾结点的位置 = invalidIndex = 0
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 直接用 slotvalue 去查最近的一条消息索引
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 秒级, 用 int 表示
                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;
                        // 消息的 store time stamp
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        // 是否在查询的时间范围内 【begin，bend】
                        boolean timeMatched = timeRead >= begin && timeRead <= end;

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}

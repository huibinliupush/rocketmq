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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TimerWheel {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final int BLANK = -1, IGNORE = -2;
    // 7天
    // 一秒(延时精度)一个 slot
    public final int slotsTotal;
    // 1000
    public final int precisionMs;
    // user.home/store/timerwheel
    private String fileName;
    // 文件大小 wheelLength
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    // 文件大小 wheelLength
    private final MappedByteBuffer mappedByteBuffer;
    // 大小为 wheelLength，初始化的时候会将 mappedByteBuffer 中的内容写入到该 buffer 中
    // 存储 timer wheel
    private final ByteBuffer byteBuffer;
    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            // 存储 timer wheel
            return byteBuffer.duplicate();
        }
    };
    // 双缓冲区：每个逻辑槽位实际对应两个物理槽位（例如，逻辑槽 i 对应物理索引 i 和 i + slotsTotal）
    // 当前轮次写入物理索引的低区（0 ~ slotsTotal-1），下一轮次写入高区（slotsTotal ~ 2*slotsTotal-1）
    // timeMs / precisionMs 得到从起始时间起经过的 tick 数（每个 tick 长度为 precisionMs）。
    // 对 slotsTotal * 2 取模，将 tick 映射到 [0, 2*slotsTotal-1] 的物理槽位上。
    // 假设 precisionMs = 1000（1秒），slotsTotal = 3600（覆盖1小时），则物理槽数组长度为 7200。
    // 第1秒（tick=1）映射到索引 1 % 7200 = 1（低区）。
    // 第3601秒（tick=3601）映射到 3601 % 7200 = 3601（高区，对应逻辑槽 1 的下一轮缓冲区）。
    // 这种设计使得时间轮在 tick 滚动时仅需切换读写区域，而无需迁移已有消息，显著提升了定时消息的吞吐量。

    // 如果只是对 slotsTotal 取余，那么实际情况下同一个 slot 槽位可能包含不同轮次的定时任务，比如上面的例子
    // 延时 1s 的和延时 3601s 的都会落在同一个 slot 中，所以 wheelLength 设计成 2 * slotsTotal
    // （0 ~ slotsTotal-1）存放当前轮次的延时消息，（slotsTotal ~ 2*slotsTotal-1）存放下一轮次的延时消息
    /**
     * slotsTotal 是支持 7 天时间维度的时间轮表盘，rocketmq 不像 netty , kafka 那样
     * netty 在 timerTask 中设计了延时轮数，表明当前处于第几轮，延时时间可以超过 256 大小的表盘
     * kafka 则是采用多级时间轮，超过表盘轮次会添加到下一轮表盘中
     * rocketmq 中的延时任务数据结构没有设计只是轮数的属性，比如在 0 时刻，添加一个 0 延时的任务和添加一个延时 7 天的任务都会被添加
     * 到同一个 slot 中，而 slot 中的数据结构并没有涉及表示时间轮数的属性，所以每一个刻度都要有两个 % (slotsTotal * 2)
     * 因为在每一个时间刻度下都有可能出现添加一个延时 7 天的消息（不只是 0 时刻，其他时刻也是一样），
     * 如果不是 slotsTotal * 2 ，那么该刻度中就会出现延时 7 天的消息，这样是不行的，直接执行了
     * 所以要 slotsTotal * 2，延时 7 天的任务可以放到对应的 （slotsTotal ~ 2*slotsTotal-1）
     *
     * */
    private final int wheelLength;

    public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
        // 7天
        // 一秒(延时精度)一个 slot
        this.slotsTotal = slotsTotal;
        // 1000
        this.precisionMs = precisionMs;
        // user.home/store/timerwheel
        this.fileName = fileName;
        // 双缓冲区：每个逻辑槽位实际对应两个物理槽位（例如，逻辑槽 i 对应物理索引 i 和 i + slotsTotal）
        // 当前轮次写入物理索引的低区（0 ~ slotsTotal-1），下一轮次写入高区（slotsTotal ~ 2*slotsTotal-1）
        // timeMs / precisionMs 得到从起始时间起经过的 tick 数（每个 tick 长度为 precisionMs）。
        // 对 slotsTotal * 2 取模，将 tick 映射到 [0, 2*slotsTotal-1] 的物理槽位上。
        // 假设 precisionMs = 1000（1秒），slotsTotal = 3600（覆盖1小时），则物理槽数组长度为 7200。
        // 第1秒（tick=1）映射到索引 1 % 7200 = 1（低区）。
        // 第3601秒（tick=3601）映射到 3601 % 7200 = 3601（高区，对应逻辑槽 1 的下一轮缓冲区）。
        // 这种设计使得时间轮在 tick 滚动时仅需切换读写区域，而无需迁移已有消息，显著提升了定时消息的吞吐量。

        // 如果只是对 slotsTotal 取余，那么实际情况下同一个 slot 槽位可能包含不同轮次的定时任务，比如上面的例子
        // 延时 1s 的和延时 3601s 的都会落在同一个 slot 中，所以 wheelLength 设计成 2 * slotsTotal
        // （0 ~ slotsTotal-1）存放当前轮次的延时消息，（slotsTotal ~ 2*slotsTotal-1）存放下一轮次的延时消息
        /**
         * slotsTotal 是支持 7 天时间维度的时间轮表盘，rocketmq 不像 netty , kafka 那样
         * netty 在 timerTask 中设计了延时轮数，表明当前处于第几轮，延时时间可以超过 256 大小的表盘
         * kafka 则是采用多级时间轮，超过表盘轮次会添加到下一轮表盘中
         * rocketmq 中的延时任务数据结构没有设计指示轮数的属性，比如在 0 时刻，添加一个 0 延时的任务和添加一个延时 7 天的任务都会被添加
         * 到同一个 slot 中，而 slot 中的数据结构并没有涉及表示时间轮数的属性，所以每一个刻度都要有两个 % (slotsTotal * 2)
         * 因为在每一个时间刻度下都有可能出现添加一个延时 7 天的消息（不只是 0 时刻，其他时刻也是一样），
         * 如果不是 slotsTotal * 2 ，那么该刻度中就会出现延时 7 天的消息，这样是不行的，直接执行了
         * 所以要 slotsTotal * 2，延时 7 天的任务可以放到对应的 （slotsTotal ~ 2*slotsTotal-1）
         *
         * */
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;

        File file = new File(fileName);
        UtilAll.ensureDirOK(file.getParent());

        try {
            randomAccessFile = new RandomAccessFile(this.fileName, "rw");
            if (file.exists() && randomAccessFile.length() != 0 &&
                randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), wheelLength));
            }
            randomAccessFile.setLength(wheelLength);
            fileChannel = randomAccessFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
            assert wheelLength == mappedByteBuffer.remaining();
            // 大小为 wheelLength，初始化的时候会将 mappedByteBuffer 中的内容写入到该 buffer 中
            // 存储 timer wheel
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(mappedByteBuffer);
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        if (flush)
            this.flush();

        // unmap mappedByteBuffer
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.byteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer wheel", e);
        }
    }
    // localBuffer 写入 mappedByteBuffer 然后 force
    public void flush() {
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(wheelLength);
        for (int i = 0; i < wheelLength; i++) {
            if (bf.get(i) != mappedByteBuffer.get(i)) {
                mappedByteBuffer.put(i, bf.get(i));
            }
        }
        this.mappedByteBuffer.force();
    }

    public Slot getSlot(long timeMs) {
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            // 空的 slot
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        // 大小为 wheelLength，初始化的时候会将 mappedByteBuffer 中的内容写入到该 buffer 中
        // wheelLength = slotsTotal * 2 * Slot.SIZE
        // 存储 timer wheel
        // 设置 position，定位 timeMs 所在的 slot index
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // 读取 timeMs 对应在 timer wheel 中的 slot
        return new Slot(localBuffer.get().getLong() * precisionMs,
            localBuffer.get().getLong(), localBuffer.get().getLong(), localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        // 双缓冲区：每个逻辑槽位实际对应两个物理槽位（例如，逻辑槽 i 对应物理索引 i 和 i + slotsTotal）
        // 当前轮次写入物理索引的低区（0 ~ slotsTotal-1），下一轮次写入高区（slotsTotal ~ 2*slotsTotal-1）
        // timeMs / precisionMs 得到从起始时间起经过的 tick 数（每个 tick 长度为 precisionMs）。
        // 对 slotsTotal * 2 取模，将 tick 映射到 [0, 2*slotsTotal-1] 的物理槽位上。
        // 假设 precisionMs = 1000（1秒），slotsTotal = 3600（覆盖1小时），则物理槽数组长度为 7200。
        // 第1秒（tick=1）映射到索引 1 % 7200 = 1（低区）。
        // 第3601秒（tick=3601）映射到 3601 % 7200 = 3601（高区，对应逻辑槽 1 的下一轮缓冲区）。
        // 这种设计使得时间轮在 tick 滚动时仅需切换读写区域，而无需迁移已有消息，显著提升了定时消息的吞吐量。

        // 如果只是对 slotsTotal 取余，那么实际情况下同一个 slot 槽位可能包含不同轮次的定时任务，比如上面的例子
        // 延时 1s 的和延时 3601s 的都会落在同一个 slot 中，所以 wheelLength 设计成 2 * slotsTotal
        // （0 ~ slotsTotal-1）存放当前轮次的延时消息，（slotsTotal ~ 2*slotsTotal-1）存放下一轮次的延时消息
        /**
         * slotsTotal 是支持 7 天时间维度的时间轮表盘，rocketmq 不像 netty , kafka 那样
         * netty 在 timerTask 中设计了延时轮数，表明当前处于第几轮，延时时间可以超过 256 大小的表盘
         * kafka 则是采用多级时间轮，超过表盘轮次会添加到下一轮表盘中
         * rocketmq 中的延时任务数据结构没有设计只是轮数的属性，比如在 0 时刻，添加一个 0 延时的任务和添加一个延时 7 天的任务都会被添加
         * 到同一个 slot 中，而 slot 中的数据结构并没有涉及表示时间轮数的属性，所以每一个刻度都要有两个 % (slotsTotal * 2)
         * 因为在每一个时间刻度下都有可能出现添加一个延时 7 天的消息（不只是 0 时刻，其他时刻也是一样），
         * 如果不是 slotsTotal * 2 ，那么该刻度中就会出现延时 7 天的消息，这样是不行的，直接执行了
         * 所以要 slotsTotal * 2，延时 7 天的任务可以放到对应的 （slotsTotal ~ 2*slotsTotal-1）
         *
         * */
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void putSlot(long timeMs, long firstPos, long lastPos) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms and it store timeMs / 1000 in slot.
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
    }
    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        // 定位 timeMs 所在 slot 的位置
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs); // 延时时间 deliver_time
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);// 如果是 delete 消息这里会减一
        localBuffer.get().putInt(magic);
    }

    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        // 定位 timeMs 对应的 slot 位置
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // 修正 slot 中的延时时间 timeMs
        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else {
            if (IGNORE != firstPos) {
                localBuffer.get().putLong(firstPos);
            } else {
                localBuffer.get().getLong(); // firstPos
            }
            if (IGNORE != lastPos) {
                localBuffer.get().putLong(lastPos); // lastPos
            }
        }
    }

    //check the timerwheel to see if its stored offset > maxOffset in timerlog
    // 所有 (lastPos > maxOffset) 的 slot 中最小的 firstPos
    // timer wheel 中的 slot 中存储的 firstPos ，lastPos 可能比 timerlog 中
    // 的 maxOffset 还要大，这种情况的发生可能是由于timerlog损坏引起的，
    // 所以这里要遍历 timer wheel 中的所有 slot, 挨个检查他们的firstPos ，lastPos
    // 重新查找 timerlog 的 recoverPos, 寻找出所有 lastPos > maxOffset 的 slot
    // 这些都是不正常的 slot, 近而找出这些不正常 slot 中最小的 firstPos
    // 后续 timerlog 将从这个 firstpos 重新恢复
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        // 检查 timer wheel 中的所有 slot
        for (int i = 0; i < slotsTotal * 2; i++) {
            // 从 currReadTime 对应的 slot 开始遍历所有 slot
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            // 获取 slot 的 timeMs
            if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }
            // slot 中第一个延时消息在 timer log 中的 offset
            long first = localBuffer.get().getLong();
            // slot 中最后一个延时消息在 timer log 中的 offset
            long last = localBuffer.get().getLong();
            // last 超过了 timerlog 中最大的 offset
            if (last > maxOffset) {
                if (first < minFirst) {
                    minFirst = first;
                }
            }
        }
        // 所有 (lastPos > maxOffset) 的 slot 中最小的 firstPos
        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
                localBuffer.get().getLong(); //first pos
                localBuffer.get().getLong(); //last pos
                allNum = allNum + localBuffer.get().getInt();
            }
        }
        return allNum;
    }
}

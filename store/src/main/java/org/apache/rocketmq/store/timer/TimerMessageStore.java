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

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsManager;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.util.PerfCounter;

public class TimerMessageStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
    private volatile int state = INITIAL;
    // rmq_sys_wheel_timer 只有一个队列
    public static final String TIMER_TOPIC = TopicValidator.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_OUT_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    // 进入 enqueuePutQueue 的时间戳
    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    // 从时间轮中取出，准备投递到 real topic 时的时间戳
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    // 延时消息 roll 的次数，初始为 1， 每 roll 一次增加 1
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    // 延时消息取消的消息，设置 TIMER_DELETE_UNIQUE_KEY (被取消的延时消息的 UNIQUE_KEY )
    public static final String TIMER_DELETE_UNIQUE_KEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;

    public static final Random RANDOM = new Random();
    public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2;
    public static final int DAY_SECS = 24 * 3600;
    public static final int DEFAULT_CAPACITY = 1024;

    // The total days in the timer wheel when precision is 1000ms.
    // If the broker shutdown last more than the configured days, will cause message loss
    public static final int TIMER_WHEEL_TTL_DAY = 7;
    public static final int TIMER_BLANK_SLOTS = 60;
    public static final int MAGIC_DEFAULT = 1;
    // 如果消息的延时时间超过 2 天 -> needRoll = true
    // 在定时消息放入时间轮前进行判断，如果在 2 天内要投递（在时间轮的时间窗口之内），则放入时间轮，否则重新放入 CommitLog 进行轮转。
    // needRoll message 到期之后重新投递到 TIMER_TOPIC,当消息的延时时间超过 2 天的时候就需要 roll （设计有点像多级时间轮）
    // 防止因为延时时间太久，commitlog 的内容被删除
    // need roll message 的 deliver time 会被重新设置，如果原来的 deliver timer - 当前时间 - timerRollWindow(2天)
    // 这个增量没超过三分之一的 timerRollWindow，那么 need roll 消息的 deliver timer 会被设置为 当前时间 + 二分之一 timerRollWindow
    // 否则设置为 当前时间 + 二分之一 timerRollWindow
    // 当 need roll message 到期之后就会来到这里，重新被投递到 TIMER_TOPIC 中，重新走一遍整个延时调度流程
    // 会再次来到 need roll 的判断，将 origin deliver timer - 当前时间  看看是否超过 timerRollWindow
    // 没有超过则按照正常延时消息投递，如果超过则在走一遍 need roll 流程
    public static final int MAGIC_ROLL = 1 << 1;
    // 延时消息取消的消息，设置 TIMER_DELETE_UNIQUE_KEY(被取消的延时消息的 UNIQUE_KEY )
    public static final int MAGIC_DELETE = 1 << 2;
    public boolean debug = false;

    protected static final String ENQUEUE_PUT = "enqueue_put";
    protected static final String DEQUEUE_PUT = "dequeue_put";
    protected final PerfCounter.Ticks perfCounterTicks = new PerfCounter.Ticks(LOGGER);
    // DEFAULT_CAPACITY = 1024
    protected final BlockingQueue<TimerRequest> enqueuePutQueue;
    // 队列元素 list 中保存的是位于同一 commitlog 文件的到期延时消息或者 delete msgs
    protected final BlockingQueue<List<TimerRequest>> dequeueGetQueue;
    protected final BlockingQueue<TimerRequest> dequeuePutQueue;
    // 4k
    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);
    // 每个线程 4M + 100 大小的 bufferLocal
    private final ThreadLocal<ByteBuffer> bufferLocal;
    // SingleThread
    private final ScheduledExecutorService scheduler;

    private final MessageStore messageStore;
    // user.home/store/timerwheel
    // 文件大小为 wheelLength = slotsTotal * 2 * Slot.SIZE
    private final TimerWheel timerWheel;
    // user.home/stpre/timerlog  100M
    private final TimerLog timerLog;
    // user.home/store/config/timercheck
    private final TimerCheckpoint timerCheckpoint;
    // 从 TIMER_TOPIC queue 中不断的消费延时消息，封装 TimerRequest 并加入到 enqueuePutQueue
    private TimerEnqueueGetService enqueueGetService;
    // 从 enqueuePutQueue 中拉取最多 10 个 TimerRequest
    // TimerRequest 写入 timerlog ，更新 timerwheel 对应 slot 信息
    // 更新 commitQueueOffset
    private TimerEnqueuePutService enqueuePutService;
    // 啥也不做
    private TimerDequeueWarmService dequeueWarmService;
    // 将 currReadTimeMs 对应在 timer wheel 中的 slot 中的延时消息全部从 timerlog 中读取出来并封装成 TimerRequest
    // 将这些 TimerRequest list 全部添加到 dequeueGetQueue 中等待处理，当 TimerRequest 全部被处理完成
    // currReadTimeMs 向前推进 precisionMs
    // 真正的时间轮转动就在这里
    private TimerDequeueGetService dequeueGetService;
    // timerPutMessageThreadNum = 3
    // 每隔 10 ms 从 dequeuePutQueue 中就到期的 TimerRequest 取出
    // 如果 TimerRequest 是正常的到期延时消息，则将其投递到 real topic 中
    // 如果是到期的 nedd roll 消息，则重新投递到 TIMER_TOPIC
    private TimerDequeuePutMessageService[] dequeuePutMessageServices;
    // timerGetMessageThreadNum = 3
    // 从 dequeueGetQueue.poll TimerRequest list(位于同一 commitlog 文件的到期延时消息或者 delete msgs)
    // 从 commitlog 中读取 TimerRequest 对应消息内容，如果是 delete msgs 则将 PROPERTY_TIMER_DEL_UNIQKEY
    // 写入到 TimerRequest 的 deleteList 中，一个 slot 中的所有 TimerRequest 共用一个 deleteList（需要被取消的延时消息）
    // 如果是 正常延时消息以及 needRoll 延时消息则判断消息是否存在 deleteList 中，如果存在则取消，不会将其放入 dequeuePutQueue 中
    // 否则进入 dequeuePutQueue
    private TimerDequeueGetMessageService[] dequeueGetMessageServices;
    // 每隔 1000 ms 执行 flush timerLog,timerWheel,timerCheckpoint
    private TimerFlushService timerFlushService;

    protected volatile long currReadTimeMs;
    // currentTimeMillis 向下取整到 precisionMs 的整数倍
    // 向 timer wheel 插入延时消息的时间轮指针
    protected volatile long currWriteTimeMs;
    protected volatile long preReadTimeMs;
    protected volatile long commitReadTimeMs;
    // TIMER_TOPIC queue 的当前处理位置
    // 每当从 queue 中读取出一个延时消息并将其 enqueuePutQueue 之后这里就向前推进到下一个 offset
    protected volatile long currQueueOffset; //only one queue that is 0
    // TIMER_TOPIC queue 的 commitQueueOffset
    // 当 enqueuePutQueue 中没有数据可拉取，用 currQueueOffset 赋值
    // 最后一个写入 timerlog 的延时消息对应在 TIMER_TOPIC queue 中的 queueOffset
    protected volatile long commitQueueOffset;
    protected volatile long lastCommitReadTimeMs;
    protected volatile long lastCommitQueueOffset;
    // 最近一次从 TIMER_TOPIC queue 中读取到延时消息的时间戳
    private long lastEnqueueButExpiredTime;
    // 最近一次从 TIMER_TOPIC queue 中读取到的延时消息的 StoreTimestamp
    private long lastEnqueueButExpiredStoreTime;

    private final int commitLogFileSize;
    // 100M
    private final int timerLogFileSize;
    // 3600 * 24 * 2  (2天)
    private final int timerRollWindowSlots;
    // The total days in the timer wheel when precision is 1000ms.
    // If the broker shutdown last more than the configured days, will cause message loss
    // 7天
    // 一秒(延时精度)一个 slot
    private final int slotsTotal;
    // timerPrecisionMs = 1000
    protected final int precisionMs;
    protected final MessageStoreConfig storeConfig;
    // user.home/store/config/timermetrics
    protected TimerMetrics timerMetrics;
    protected long lastTimeOfCheckMetrics = System.currentTimeMillis();
    protected AtomicInteger frequency = new AtomicInteger(0);

    private volatile BrokerRole lastBrokerRole = BrokerRole.SLAVE;
    //the dequeue is an asynchronous process, use this flag to track if the status has changed
    private boolean dequeueStatusChangeFlag = false;
    private long shouldStartTime;

    // True if current store is master or current brokerId is equal to the minimum brokerId of the replica group in slaveActingMaster mode.
    protected volatile boolean shouldRunningDequeue;
    private final BrokerStatsManager brokerStatsManager;
    // escapeBridge.putMessage(msg)
    private Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook;

    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
        TimerCheckpoint timerCheckpoint, TimerMetrics timerMetrics,
        final BrokerStatsManager brokerStatsManager) throws IOException {

        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.commitLogFileSize = storeConfig.getMappedFileSizeCommitLog();
        // 100M
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
        // timerPrecisionMs = 1000
        this.precisionMs = storeConfig.getTimerPrecisionMs();

        // TimerWheel contains the fixed number of slots regardless of precision.
        // The total days in the timer wheel when precision is 1000ms.
        // If the broker shutdown last more than the configured days, will cause message loss
        // 7天
        // 一秒(延时精度)一个 slot
        this.slotsTotal = TIMER_WHEEL_TTL_DAY * DAY_SECS;
        // user.home/store/timerwheel 文件大小为 wheelLength = slotsTotal * 2 * Slot.SIZE
        this.timerWheel = new TimerWheel(
            getTimerWheelPath(storeConfig.getStorePathRootDir()), this.slotsTotal, precisionMs);
        // user.home/stpre/timerlog  100M
        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        // user.home/store/config/timermetrics
        this.timerMetrics = timerMetrics;
        // user.home/store/config/timercheck
        this.timerCheckpoint = timerCheckpoint;
        this.lastBrokerRole = storeConfig.getBrokerRole();

        if (messageStore instanceof DefaultMessageStore) {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("TimerScheduledThread",
                    ((DefaultMessageStore) messageStore).getBrokerIdentity()));
        } else {
            scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("TimerScheduledThread"));
        }

        // timerRollWindow contains the fixed number of slots regardless of precision.
        // timerRollWindowSlot = 3600 * 24 * 2  (2天)
        // TIMER_BLANK_SLOTS = 60
        if (storeConfig.getTimerRollWindowSlot() > slotsTotal - TIMER_BLANK_SLOTS
            || storeConfig.getTimerRollWindowSlot() < 2) {
            this.timerRollWindowSlots = slotsTotal - TIMER_BLANK_SLOTS;
        } else {
            this.timerRollWindowSlots = storeConfig.getTimerRollWindowSlot();
        }
        // 4M + 100
        bufferLocal = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                // maxMessageSize = 4M (message body）
                return ByteBuffer.allocateDirect(storeConfig.getMaxMessageSize() + 100);
            }
        };
        // false
        if (storeConfig.isTimerEnableDisruptor()) {
            enqueuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            dequeueGetQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
            dequeuePutQueue = new DisruptorBlockingQueue<>(DEFAULT_CAPACITY);
        } else {
            // DEFAULT_CAPACITY = 1024
            enqueuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            dequeueGetQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
            dequeuePutQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        }
        this.brokerStatsManager = brokerStatsManager;
    }
    // 总共开启 10 个线程来进行延时消息的调度
    public void initService() {
        // TIMER_TOPIC 的消费者
        enqueueGetService = new TimerEnqueueGetService();
        // 将延时消息写入 timerlog timer wheel
        enqueuePutService = new TimerEnqueuePutService();
        // 空实现
        dequeueWarmService = new TimerDequeueWarmService();
        // 推进时间轮的转动，拉取 currentReadTime 对应 slot 中的所有延时任务
        // 过滤出没有被取消的延时消息以及 need roll 延时消息
        dequeueGetService = new TimerDequeueGetService();
        // flush timerlog,timerWheel,timerCheckPoint
        timerFlushService = new TimerFlushService();
        // timerGetMessageThreadNum = 3
        int getThreadNum = Math.max(storeConfig.getTimerGetMessageThreadNum(), 1);
        // 三个线程负责从 dequeueGetQueue 中取出到期的延时消息，过滤出要被取消的延时消息和正常延时消息以及 need roll 消息
        // 将他们归类，并将归类后的 list 写入到 dequeuePutQueue 中
        dequeueGetMessageServices = new TimerDequeueGetMessageService[getThreadNum];
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i] = new TimerDequeueGetMessageService();
        }
        // timerPutMessageThreadNum = 3
        int putThreadNum = Math.max(storeConfig.getTimerPutMessageThreadNum(), 1);
        // 三个线程负责从 dequeuePutQueue 中取出到期的延时消息，将延时消息写入 real topic 中
        // 将 need roll 消息重新写入 TIMER_TOPIC(防止 commitlog 过期，导致其中的延时消息被删除)
        dequeuePutMessageServices = new TimerDequeuePutMessageService[putThreadNum];
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            dequeuePutMessageServices[i] = new TimerDequeuePutMessageService();
        }
    }

    public boolean load() {
        // 总共开启 10 个线程来进行延时消息的调度
        this.initService();
        // 从磁盘 user.home/stpre/timerlog  加载所有 timer log 文件
        boolean load = timerLog.load();
        load = load && this.timerMetrics.load();
        recover();
        calcTimerDistribution();
        return load;
    }

    public static String getTimerWheelPath(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public static String getTimerLogPath(final String rootDir) {
        return rootDir + File.separator + "timerlog";
    }

    private void calcTimerDistribution() {
        long startTime = System.currentTimeMillis();
        List<Integer> timerDist = this.timerMetrics.getTimerDistList();
        // 当前时间戳向下取整为 precisionMs 的整数倍
        long currTime = System.currentTimeMillis() / precisionMs * precisionMs;
        for (int i = 0; i < timerDist.size(); i++) {
            int slotBeforeNum = i == 0 ? 0 : timerDist.get(i - 1) * 1000 / precisionMs;
            int slotTotalNum = timerDist.get(i) * 1000 / precisionMs;
            int periodTotal = 0;
            for (int j = slotBeforeNum; j < slotTotalNum; j++) {
                Slot slotEach = timerWheel.getSlot(currTime + (long) j * precisionMs);
                periodTotal += slotEach.num;
            }
            LOGGER.debug("{} period's total num: {}", timerDist.get(i), periodTotal);
            this.timerMetrics.updateDistPair(timerDist.get(i), periodTotal);
        }
        long endTime = System.currentTimeMillis();
        LOGGER.debug("Total cost Time: {}", endTime - startTime);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void recover() {
        //recover timerLog
        // timer log 全局 flushwhere
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            // lastFlushPos 向前回退一个 file size
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        // 如果只有一个 timer log 文件，那么 lastFlushPos 回退到文件开始处
        if (lastFlushPos < 0) {
            lastFlushPos = 0;
        }
        // 从 lastFlushPos 所在的 timer log 文件开始逐个向后 recover
        // 挨个检查 timer log 文件中的 unit, 如果发现无效 unit (TimerLog.UNIT_SIZE != size))
        // 则从该无效 unit 位置处向后截断 timer log
        // 如果是有效 unit, 则修正 unit 中 delayTime 对应的 slot
        // slot 中的 timeMs 修正为 delayTime ， lastPos 修正为当前遍历到的 timer log unit 位置(sbr.getStartOffset() + position)
        // processOffset 之前的 timer log unit 全部都是有效的
        long processOffset = recoverAndRevise(lastFlushPos, true);
        // 重新设置全局 flushWhere
        timerLog.getMappedFileQueue().setFlushedWhere(processOffset);
        //revise queue offset
        // 修正 timer log 中最后一个有效的延时消息在 TIMER_TOPIC QUEUE 中的 queueOffset
        // timer_uint->(offsetPy,sizePy)->commitlog(MessageExt)->QueueOffset->ConsumeQueue(比较 offsetPy 是否相同)
        // 不相同则继续在 ConsumeQueue 中向前查找直到找到相同的 offsetPy，那么此时的 queueOffset 就找到了
        long queueOffset = reviseQueueOffset(processOffset);
        if (-1 == queueOffset) {
            // 没找到按照 timerCheckPoint 中保存的来
            currQueueOffset = timerCheckpoint.getLastTimerQueueOffset();
        } else {
            // 找到了则从下一个 offset 开始，该从哪个位置开始消费 TIMER_TOPIC QUEUE
            // queueOffset 为 timer log 中最后一个有效的 unit 在 TIMER_TOPIC QUEUE 的 offset
            currQueueOffset = queueOffset + 1;
        }
        currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());

        ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, 0);

        // Correction based consume queue
        if (cq != null && currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                currQueueOffset, cq.getMinOffsetInQueue());
            currQueueOffset = cq.getMinOffsetInQueue();
        } else if (cq != null && currQueueOffset > cq.getMaxOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is larger than maxOffsetInQueue:{}",
                currQueueOffset, cq.getMaxOffsetInQueue());
            currQueueOffset = cq.getMaxOffsetInQueue();
        }

        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        // 当前时间向前推 7天
        long nextReadTimeMs = formatTimeMs(
            System.currentTimeMillis()) - (long) slotsTotal * precisionMs + (long) TIMER_BLANK_SLOTS * precisionMs;
        if (currReadTimeMs < nextReadTimeMs) {
            // 如果 currReadTimeMs 落后当前时间超过 7 天
            currReadTimeMs = nextReadTimeMs;
        }
        //the timer wheel may contain physical offset bigger than timerLog
        //This will only happen when the timerLog is damaged
        //hard to test
        // check the timerwheel to see if its stored offset > maxOffset in timerlog
        // 所有 (lastPos > maxOffset) 的 slot 中最小的 firstPos
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs, processOffset);
        if (debug) {
            minFirst = 0;
        }
        if (minFirst < processOffset) {
            LOGGER.warn("Timer recheck because of minFirst:{} processOffset:{}", minFirst, processOffset);
            // 从 minFirst 所在的 timer log 文件开始逐个向后 recover
            // 挨个检查 timer log 文件中的 unit, 如果发现无效 unit (TimerLog.UNIT_SIZE != size))
            // 这里不截断(则从该无效 unit 位置处向后截断 timer log)
            // 如果是有效 unit, 则修正 unit 中 delayTime 对应的 slot
            // slot 中的 timeMs 修正为 delayTime ， lastPos 修正为当前遍历到的 timer log unit 位置(sbr.getStartOffset() + position)
            recoverAndRevise(minFirst, false); // 主要是修正从 minFirst 开始的每个 unit 对应的 slot 的 lastPos
        }
        LOGGER.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{} processOffset:{}",
            currReadTimeMs, currQueueOffset, timerCheckpoint.getLastTimerQueueOffset(), processOffset);

        commitReadTimeMs = currReadTimeMs;
        commitQueueOffset = currQueueOffset;

        prepareTimerCheckPoint();
    }
    // processOffset 是 timer log 截断之后的位置，processOffset 之后的 timer log 全部截断，之前的 timer log 全部都是有效的 unit
    // 修正 timer log 中最后一个有效的延时消息在 TIMER_TOPIC QUEUE 中的 queueOffset
    // timer_uint->(offsetPy,sizePy)->commitlog(MessageExt)->QueueOffset->ConsumeQueue(比较 offsetPy 是否相同)
    // 不相同则继续在 ConsumeQueue 中向前查找直到找到相同的 offsetPy，那么此时的 queueOffset 就找到了
    public long reviseQueueOffset(long processOffset) {
        // processOffset 前一个 unit 的  offsetPy （在 commitlog 中的 offset）
        // 最后一个有效 unit 的 offsetPy
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - (TimerLog.UNIT_SIZE - TimerLog.UNIT_PRE_SIZE_FOR_MSG));
        if (null == selectRes) {
            return -1;
        }
        try {
            // 最后一个有效 unit 的 offsetPy
            long offsetPy = selectRes.getByteBuffer().getLong();
            // 最后一个有效 unit 的 sizePy
            int sizePy = selectRes.getByteBuffer().getInt();
            // timer log 中最后一个有效的延时消息内容
            MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
            if (null == messageExt) {
                return -1;
            }

            // check offset in msg is equal to offset of cq.
            // if not, use cq offset.
            // 在 TIMER_TOPIC queue 中的 QueueOffset
            long msgQueueOffset = messageExt.getQueueOffset();
            // 0 (TIMER_TOPIC 只有一个 queue)
            int queueId = messageExt.getQueueId();
            ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
            if (null == cq) {
                return msgQueueOffset;
            }
            // timer log 中最后一个有效延时消息在 TIMER_TOPIC queue 中的 QueueOffset
            long cqOffset = msgQueueOffset;
            long tmpOffset = msgQueueOffset;
            int maxCount = 20000;
            while (maxCount-- > 0) {
                if (tmpOffset < 0) {
                    LOGGER.warn("reviseQueueOffset check cq offset fail, msg in cq is not found.{}, {}",
                        offsetPy, sizePy);
                    break;
                }
                ReferredIterator<CqUnit> iterator = null;
                try {
                    // 在 TIMER_TOPIC queue 中获取延时消息索引
                    iterator = cq.iterateFrom(tmpOffset);
                    CqUnit cqUnit = null;
                    // 消息索引不存在，则继续向前遍历 consume queue
                    if (null == iterator || (cqUnit = iterator.next()) == null) {
                        // offset in msg may be greater than offset of cq.
                        tmpOffset -= 1;
                        continue;
                    }

                    long offsetPyTemp = cqUnit.getPos();
                    int sizePyTemp = cqUnit.getSize();
                    if (offsetPyTemp == offsetPy && sizePyTemp == sizePy) {
                        LOGGER.info("reviseQueueOffset check cq offset ok. {}, {}, {}",
                            tmpOffset, offsetPyTemp, sizePyTemp);
                        // 找到延时消息正确的 consume queue offset
                        cqOffset = tmpOffset;
                        break;
                    }
                    // 继续向前找
                    tmpOffset -= 1;
                } catch (Throwable e) {
                    LOGGER.error("reviseQueueOffset check cq offset error.", e);
                } finally {
                    if (iterator != null) {
                        iterator.release();
                    }
                }
            }

            return cqOffset;
        } finally {
            selectRes.release();
        }
    }

    //recover timerLog and revise timerWheel
    //return process offset
    // 从 beginOffset 所在的 timer log 文件开始逐个向后 recover
    // 挨个检查 timer log 文件中的 unit, 如果发现无效 unit (TimerLog.UNIT_SIZE != size))
    // 则从该无效 unit 位置处向后截断 timer log
    // 如果是有效 unit, 则修正 unit 中 delayTime 对应的 slot
    // slot 中的 timeMs 修正为 delayTime ， lastPos 修正为当前遍历到的 timer log unit 位置(sbr.getStartOffset() + position)
    private long recoverAndRevise(long beginOffset, boolean checkTimerLog) {
        LOGGER.info("Begin to recover timerLog offset:{} check:{}", beginOffset, checkTimerLog);
        // beginOffset 为 timerCheckpoint 中的 LastTimerLogFlushPos 向前回退一个 filesize
        // 如果只有一个 timer log 文件则从 0 开始
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null == lastFile) {
            return 0;
        }
        // 获取所有 timerlog 文件
        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        // 从最后一个 timer log 文件开始查找需要 recover 的第一个文件
        int index = mappedFiles.size() - 1;
        for (; index >= 0; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            // 从 beginOffset 所在的 timer log 文件开始逐个向后 recover
            if (beginOffset >= mappedFile.getFileFromOffset()) {
                break;
            }
        }
        if (index < 0) {
            index = 0;
        }
        // 第一个开始 recover 的 timer log 文件
        long checkOffset = mappedFiles.get(index).getFileFromOffset();
        // 依次向后 recover timer log 文件
        for (; index < mappedFiles.size(); index++) {
            MappedFile mappedFile = mappedFiles.get(index);
            // checkTimerLog = true ： 获取整个 timerlog 文件内容
            // checkTimerLog = fasle ： 获取 timerlog [0,readPosition] 文件内容
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, checkTimerLog ? mappedFiles.get(index).getFileSize() : mappedFile.getReadPosition());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            // 一个 unit 一个 unit 的检查
            for (; position < sbr.getSize(); position += TimerLog.UNIT_SIZE) {
                try {
                    bf.position(position);
                    int size = bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    // timerlog 文件末尾表示
                    if (magic == TimerLog.BLANK_MAGIC_CODE) {
                        break;
                    }
                    // size 要与 UNIT_SIZE 相同，否则停止 check, 找打了无效 unit
                    if (checkTimerLog && (!isMagicOK(magic) || TimerLog.UNIT_SIZE != size)) {
                        stopCheck = true;
                        break;
                    }
                    // curr write time + delayed time
                    long delayTime = bf.getLong() + bf.getInt();
                    if (TimerLog.UNIT_SIZE == size && isMagicOK(magic)) {
                        // 修正 delayTime 对应的 slot
                        // slot 中的 timeMs 修正为 delayTime ， lastPos 修正为当前遍历到的 timer log unit 位置(sbr.getStartOffset() + position)
                        timerWheel.reviseSlot(delayTime, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
                } catch (Exception e) {
                    LOGGER.error("Recover timerLog error", e);
                    stopCheck = true;
                    break;
                }
            }
            sbr.release();
            // 表示当前 checkd 到了 timer log 的哪里（全局）
            checkOffset = mappedFiles.get(index).getFileFromOffset() + position;
            // 找到无效的 timer log unit 则停止检查
            if (stopCheck) {
                break;
            }
        }
        if (checkTimerLog) {
            // checkOffset 后面的全部都是无效的 unit,截断
            timerLog.getMappedFileQueue().truncateDirtyFiles(checkOffset);
        }
        // checkOffset 之前的 timer log unit 全部都是有效的
        return checkOffset;
    }

    public static boolean isMagicOK(int magic) {
        return (magic | 0xF) == 0xF;
    }

    public void start() {
        this.shouldStartTime = storeConfig.getDisappearTimeAfterStart() + System.currentTimeMillis();
        // 推进 currWriteTimeMs （向下取整 precisionMs 的整数倍）
        maybeMoveWriteTime();
        // 从 TIMER_TOPIC queue 中不断的消费延时消息，封装 TimerRequest 并加入到 enqueuePutQueue
        enqueueGetService.start();
        // 从 enqueuePutQueue 中拉取最多 10 个 TimerRequest
        // TimerRequest 写入 timerlog ，更新 timerwheel 对应 slot 信息
        // 更新 commitQueueOffset
        enqueuePutService.start();
        // 啥也不做
        dequeueWarmService.start();
        // 将 currReadTimeMs 对应在 timer wheel 中的 slot 中的延时消息全部从 timerlog 中读取出来并封装成 TimerRequest
        // 将这些 TimerRequest list 全部添加到 dequeueGetQueue 中等待处理，当 TimerRequest 全部被处理完成
        // currReadTimeMs 向前推进 precisionMs
        // 真正的时间轮转动就在这里
        dequeueGetService.start();
        // timerGetMessageThreadNum = 3
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            // 从 dequeueGetQueue.poll TimerRequest list(位于同一 commitlog 文件的到期延时消息或者 delete msgs)
            // 从 commitlog 中读取 TimerRequest 对应消息内容，如果是 delete msgs 则将 PROPERTY_TIMER_DEL_UNIQKEY
            // 写入到 TimerRequest 的 deleteList 中，一个 slot 中的所有 TimerRequest 共用一个 deleteList（需要被取消的延时消息）
            // 如果是 正常延时消息以及 needRoll 延时消息则判断消息是否存在 deleteList 中，如果存在则取消，不会将其放入 dequeuePutQueue 中
            // 否则进入 dequeuePutQueue
            dequeueGetMessageServices[i].start();
        }
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            // 每隔 10 ms 从 dequeuePutQueue 中就到期的 TimerRequest 取出
            // 如果 TimerRequest 是正常的到期延时消息，则将其投递到 real topic 中
            // 如果是到期的 nedd roll 消息，则重新投递到 TIMER_TOPIC
            dequeuePutMessageServices[i].start();
        }
        // 每隔 1000 ms 执行 flush timerLog,timerWheel,timerCheckpoint
        timerFlushService.start();
        // 每隔 30s 清理过期的 timer log mappedFile
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 每隔 30s
                    long minPy = messageStore.getMinPhyOffset(); // commitlog 最小 offsetPy
                    // 计算出 timer log 最后一个 unit 的 offset
                    int checkOffset = timerLog.getOffsetForLastUnit();
                    // commitlog 文件如果过期了，那么 commitlog 中的延时消息所在的 timer log 也应该过期清理
                    // 挨个遍历 timer log 的 mappedFile
                    // 如果timerlog mappedFile中最后一个延时消息的 offsetPy 小于 commitlog 最小 offsetPy
                    // 说明整个 timerlog mappedFile 过期，需要清理
                    timerLog.getMappedFileQueue()
                        .deleteExpiredFileByOffsetForTimerLog(minPy, checkOffset, TimerLog.UNIT_SIZE);
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    // 每隔 45 分钟
                    if (storeConfig.isTimerEnableCheckMetrics()) { // true
                        // 05
                        String when = storeConfig.getTimerCheckMetricsWhen();
                        // 是否到达凌晨 5 点
                        if (!UtilAll.isItTimeToDo(when)) {
                            return;
                        }
                        long curr = System.currentTimeMillis();
                        // 是否超过 70 分钟未检查
                        if (curr - lastTimeOfCheckMetrics > 70 * 60 * 1000) {
                            lastTimeOfCheckMetrics = curr;
                            checkAndReviseMetrics();
                            LOGGER.info("[CheckAndReviseMetrics]Timer do check timer metrics cost {} ms",
                                System.currentTimeMillis() - curr);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in cleaning timerLog", e);
                }
            }
        }, 45, 45, TimeUnit.MINUTES);

        state = RUNNING;
        LOGGER.info("Timer start ok currReadTimerMs:[{}] queueOffset:[{}]", new Timestamp(currReadTimeMs), currQueueOffset);
    }

    public void start(boolean shouldRunningDequeue) {
        this.shouldRunningDequeue = shouldRunningDequeue;
        this.start();
    }

    public void shutdown() {
        if (SHUTDOWN == state) {
            return;
        }
        state = SHUTDOWN;
        //first save checkpoint
        prepareTimerCheckPoint();
        timerFlushService.shutdown();
        timerLog.shutdown();
        timerCheckpoint.shutdown();

        enqueuePutQueue.clear(); //avoid blocking
        dequeueGetQueue.clear(); //avoid blocking
        dequeuePutQueue.clear(); //avoid blocking

        enqueueGetService.shutdown();
        enqueuePutService.shutdown();
        dequeueWarmService.shutdown();
        dequeueGetService.shutdown();
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i].shutdown();
        }
        for (int i = 0; i < dequeuePutMessageServices.length; i++) {
            dequeuePutMessageServices[i].shutdown();
        }
        timerWheel.shutdown(false);

        this.scheduler.shutdown();
        UtilAll.cleanBuffer(this.bufferLocal.get());
        this.bufferLocal.remove();
    }

    protected void maybeMoveWriteTime() {
        // currentTimeMillis 向下取整到 precisionMs 的整数倍
        if (currWriteTimeMs < formatTimeMs(System.currentTimeMillis())) {
            currWriteTimeMs = formatTimeMs(System.currentTimeMillis());
        }
    }

    private void moveReadTime() {
        currReadTimeMs = currReadTimeMs + precisionMs;
        commitReadTimeMs = currReadTimeMs;
    }

    private boolean isRunning() {
        return RUNNING == state;
    }

    private void checkBrokerRole() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            synchronized (lastBrokerRole) {
                LOGGER.info("Broker role change from {} to {}", lastBrokerRole, currRole);
                //if change to master, do something
                if (BrokerRole.SLAVE != currRole) {
                    currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());
                    commitQueueOffset = currQueueOffset;
                    prepareTimerCheckPoint();
                    timerCheckpoint.flush();
                    currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
                    commitReadTimeMs = currReadTimeMs;
                }
                //if change to slave, just let it go
                lastBrokerRole = currRole;
            }
        }
    }

    private boolean isRunningEnqueue() {
        checkBrokerRole();
        if (!shouldRunningDequeue && !isMaster() && currQueueOffset >= timerCheckpoint.getMasterTimerQueueOffset()) {
            return false;
        }

        return isRunning();
    }

    private boolean isRunningDequeue() {
        if (!this.shouldRunningDequeue) {
            syncLastReadTimeMs();
            return false;
        }
        return isRunning();
    }

    public void syncLastReadTimeMs() {
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        commitReadTimeMs = currReadTimeMs;
    }

    public void setShouldRunningDequeue(final boolean shouldRunningDequeue) {
        this.shouldRunningDequeue = shouldRunningDequeue;
    }

    public boolean isShouldRunningDequeue() {
        return shouldRunningDequeue;
    }

    public void addMetric(MessageExt msg, int value) {
        try {
            if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
                return;
            }
            if (msg.getProperty(TIMER_ENQUEUE_MS) != null
                && NumberUtils.toLong(msg.getProperty(TIMER_ENQUEUE_MS)) == Long.MAX_VALUE) {
                return;
            }
            // pass msg into addAndGet, for further more judgement extension.
            timerMetrics.addAndGet(msg, value);
        } catch (Throwable t) {
            if (frequency.incrementAndGet() % 1000 == 0) {
                LOGGER.error("error in adding metric", t);
            }
        }

    }

    public void holdMomentForUnknownError(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ignored) {

        }
    }

    public void holdMomentForUnknownError() {
        holdMomentForUnknownError(50);
    }

    public boolean enqueue(int queueId) {
        if (storeConfig.isTimerStopEnqueue()) {
            return false;
        }
        if (!isRunningEnqueue()) {
            return false;
        }
        // 获取 TIMER_TOPIC 对应的 queue (只有一个)
        ConsumeQueueInterface cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return false;
        }
        // TIMER_TOPIC queue 的当前处理位置
        if (currQueueOffset < cq.getMinOffsetInQueue()) {
            LOGGER.warn("Timer currQueueOffset:{} is smaller than minOffsetInQueue:{}",
                currQueueOffset, cq.getMinOffsetInQueue());
            currQueueOffset = cq.getMinOffsetInQueue();
        }
        long offset = currQueueOffset;
        ReferredIterator<CqUnit> iterator = null;
        try {
            // 从 offset 位置处开始读取 queue 中的延时消息索引
            iterator = cq.iterateFrom(offset);
            if (null == iterator) {
                // TIMER_TOPIC 中没有数据
                return false;
            }

            int i = 0;
            // 遍历 TIMER_TOPIC queue
            while (iterator.hasNext()) {
                i++;
                perfCounterTicks.startTick("enqueue_get");
                try {
                    // 获取延时消息索引
                    CqUnit cqUnit = iterator.next();
                    // 延时消息在 commitlog 中的 offset
                    long offsetPy = cqUnit.getPos();
                    int sizePy = cqUnit.getSize();
                    cqUnit.getTagsCode(); //tags code
                    // 从 commitlog 中获取延时消息内容
                    MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                    if (null == msgExt) {
                        perfCounterTicks.getCounter("enqueue_get_miss");
                    } else {
                        // 最近一次从 TIMER_TOPIC queue 中读取到延时消息的时间戳
                        lastEnqueueButExpiredTime = System.currentTimeMillis();
                        // 最近一次从 TIMER_TOPIC queue 中读取到的延时消息的 StoreTimestamp
                        lastEnqueueButExpiredStoreTime = msgExt.getStoreTimestamp();
                        // 获取延时消息的 delayedTime
                        long delayedTime = Long.parseLong(msgExt.getProperty(TIMER_OUT_MS));
                        // use CQ offset, not offset in Message
                        // offset + i 为延时消息在 TIMER_TOPIC queue 中的位置
                        msgExt.setQueueOffset(offset + i);
                        // 将延时消息相关内容封装成 TimerRequest
                        TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), MAGIC_DEFAULT, msgExt);
                        // System.out.printf("build enqueue request, %s%n", timerRequest);
                        // 向 enqueuePutQueue 中添加 TimerRequest
                        // 如果 enqueuePutQueue 满了，这里会一直持续尝试添加直到添加成功
                        while (!enqueuePutQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
                            if (!isRunningEnqueue()) {
                                return false;
                            }
                        }
                        Attributes attributes = DefaultStoreMetricsManager.newAttributesBuilder()
                                .put(DefaultStoreMetricsConstant.LABEL_TOPIC, msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC)).build();
                        DefaultStoreMetricsManager.timerMessageSetLatency.record((delayedTime - msgExt.getBornTimestamp()) / 1000, attributes);
                    }
                } catch (Exception e) {
                    // here may cause the message loss
                    if (storeConfig.isTimerSkipUnknownError()) {
                        LOGGER.warn("Unknown error in skipped in enqueuing", e);
                    } else {
                        holdMomentForUnknownError();
                        throw e;
                    }
                } finally {
                    perfCounterTicks.endTick("enqueue_get");
                }
                // if broker role changes, ignore last enqueue
                if (!isRunningEnqueue()) {
                    return false;
                }
                currQueueOffset = offset + i;
            }
            // 推进 TIMER_TOPIC queue offset
            // 每当从 queue 中读取出一个延时消息并将其 enqueuePutQueue 之后这里就向前推进到下一个 offset
            currQueueOffset = offset + i;
            return i > 0;
        } catch (Exception e) {
            LOGGER.error("Unknown exception in enqueuing", e);
        } finally {
            if (iterator != null) {
                iterator.release();
            }
        }
        return false;
    }

    public boolean doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        LOGGER.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);
        //copy the value first, avoid concurrent problem
        long tmpWriteTimeMs = currWriteTimeMs;
        // 如果消息的延时时间超过 2 天 -> needRoll = true
        // 在定时消息放入时间轮前进行判断，如果在 2 天内要投递（在时间轮的时间窗口之内），则放入时间轮，否则重新放入 TIMER_TOPIC CommitLog 进行轮转。
        boolean needRoll = delayedTime - tmpWriteTimeMs >= (long) timerRollWindowSlots * precisionMs;
        int magic = MAGIC_DEFAULT;
        // needRoll message 到期之后重新投递到 TIMER_TOPIC,当消息的延时时间超过 2 天的时候就需要 roll （设计有点像多级时间轮）
        // 防止因为延时时间太久，commitlog 的内容被删除
        // need roll message 的 deliver time 会被重新设置，如果原来的 deliver timer - 当前时间 - timerRollWindow(2天)
        // 这个增量没超过三分之一的 timerRollWindow，那么 need roll 消息的 deliver timer 会被设置为 当前时间 + 二分之一 timerRollWindow
        // 否则设置为 当前时间 + 二分之一 timerRollWindow
        // 当 need roll message 到期之后就会来到这里，重新被投递到 TIMER_TOPIC 中，重新走一遍整个延时调度流程
        // 会再次来到 need roll 的判断，将 origin deliver timer - 当前时间  看看是否超过 timerRollWindow
        // 没有超过则按照正常延时消息投递，如果超过则在走一遍 need roll 流程
        if (needRoll) {
            magic = magic | MAGIC_ROLL;
            // 消息延时时间超过 2 天的增量 ： delayedTime - tmpWriteTimeMs - (long) timerRollWindowSlots * precisionMs
            // 比如延时 3 天，timerRollWindowSlots = 2 天,那么这里的增量就是 1 天
            // 如果这个增量小于 1/3 timerRollWindowSlots 也就是小于 0.6 天
            if (delayedTime - tmpWriteTimeMs - (long) timerRollWindowSlots * precisionMs < (long) timerRollWindowSlots / 3 * precisionMs) {
                //give enough time to next roll
                // 延时时间被重新设置为延时 1 天 （timerRollWindowSlots / 2）
                delayedTime = tmpWriteTimeMs + (long) (timerRollWindowSlots / 2) * precisionMs;
            } else {
                // 增量大于 0.6 天，延时时间设置为延时 2 天
                delayedTime = tmpWriteTimeMs + (long) timerRollWindowSlots * precisionMs;
            }
        }
        // 延时消息取消的消息，设置 TIMER_DELETE_UNIQUE_KEY(被取消的延时消息的 UNIQUE_KEY )
        boolean isDelete = messageExt.getProperty(TIMER_DELETE_UNIQUE_KEY) != null;
        if (isDelete) {
            magic = magic | MAGIC_DELETE;
        }
        String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
        // 通过 delayedTime 获取所在 timerWheel 的 slot
        Slot slot = timerWheel.getSlot(delayedTime);
        ByteBuffer tmpBuffer = timerLogBuffer; // 4K
        tmpBuffer.clear();
        tmpBuffer.putInt(TimerLog.UNIT_SIZE); //size = 52 , timerlog 中的 unit 同 consumerqueue 一样都是固定长度的
        tmpBuffer.putLong(slot.lastPos); //prev pos 初始为 -1
        tmpBuffer.putInt(magic); //magic
        tmpBuffer.putLong(tmpWriteTimeMs); //currWriteTime
        tmpBuffer.putInt((int) (delayedTime - tmpWriteTimeMs)); //delayTime
        tmpBuffer.putLong(offsetPy); //offset
        tmpBuffer.putInt(sizePy); //size
        tmpBuffer.putInt(hashTopicForMetrics(realTopic)); //hashcode of real topic
        tmpBuffer.putLong(0); //reserved value, just set to 0 now
        // ret 为 timerlog 开始写入的起始位置，也就是延时消息在 timerlog 中的存储 offset
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 != ret) {
            // If it's a delete message, then slot's total num -1
            // TODO: check if the delete msg is in the same slot with "the msg to be deleted".
            // 更新 slot 信息
            timerWheel.putSlot(delayedTime, slot.firstPos == -1 ? ret : slot.firstPos, ret,
                isDelete ? slot.num - 1 : slot.num + 1, slot.magic);
            addMetric(messageExt, isDelete ? -1 : 1);
        }
        return -1 != ret;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public int warmDequeue() {
        if (!isRunningDequeue()) {
            return -1;
        }
        if (!storeConfig.isTimerWarmEnable()) {
            return -1;
        }
        if (preReadTimeMs <= currReadTimeMs) {
            preReadTimeMs = currReadTimeMs + precisionMs;
        }
        if (preReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        if (preReadTimeMs >= currReadTimeMs + 3L * precisionMs) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(preReadTimeMs);
        if (-1 == slot.timeMs) {
            preReadTimeMs = preReadTimeMs + precisionMs;
            return 0;
        }
        long currOffsetPy = slot.lastPos;
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        SelectMappedBufferResult msgSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!isRunning()) {
                    break;
                }
                perfCounterTicks.startTick("warm_dequeue");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    timeSbr.getByteBuffer().position(position + TimerLog.UNIT_PRE_SIZE_FOR_MSG);
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    if (null == msgSbr || msgSbr.getStartOffset() > offsetPy) {
                        msgSbr = messageStore.getCommitLogData(offsetPy - offsetPy % commitLogFileSize);
                        if (null != msgSbr) {
                            sbrs.add(msgSbr);
                        }
                    }
                    if (null != msgSbr) {
                        ByteBuffer bf = msgSbr.getByteBuffer();
                        int firstPos = (int) (offsetPy % commitLogFileSize);
                        for (int pos = firstPos; pos < firstPos + sizePy; pos += 4096) {
                            bf.position(pos);
                            bf.get();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Unexpected error in warm", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("warm_dequeue");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        } finally {
            preReadTimeMs = preReadTimeMs + precisionMs;
        }
        return 1;
    }

    public boolean checkStateForPutMessages(int state) {
        for (AbstractStateService service : dequeuePutMessageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStateForGetMessages(int state) {
        for (AbstractStateService service : dequeueGetMessageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public void checkDequeueLatch(CountDownLatch latch, long delayedTime) throws Exception {
        // true if the count reached zero
        if (latch.await(1, TimeUnit.SECONDS)) {
            return;
        }
        int checkNum = 0;
        while (true) {
            if (dequeuePutQueue.size() > 0
                || !checkStateForGetMessages(AbstractStateService.WAITING)
                || !checkStateForPutMessages(AbstractStateService.WAITING)) {
                //let it go
            } else {
                checkNum++;
                if (checkNum >= 2) {
                    break;
                }
            }
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        if (!latch.await(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Check latch failed delayedTime:{}", delayedTime);
        }
    }
    // 将 currReadTimeMs 对应在 timer wheel 中的 slot 中的延时消息全部从 timerlog 中读取出来并封装成 TimerRequest
    // 将这些 TimerRequest list 全部添加到 dequeueGetQueue 中等待处理，当 TimerRequest 全部被处理完成
    // currReadTimeMs 向前推进 precisionMs
    public int dequeue() throws Exception {
        if (storeConfig.isTimerStopDequeue()) {
            return -1;
        }
        if (!isRunningDequeue()) {
            return -1;
        }
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        // 从时间轮中获取 currReadTimeMs 对应的 slot
        Slot slot = timerWheel.getSlot(currReadTimeMs);
        if (-1 == slot.timeMs) {
            // 向前推进 precisionMs
            moveReadTime();
            return 0;
        }
        try {
            //clear the flag
            dequeueStatusChangeFlag = false;
            // slot 中最后一个延时消息在 timerlog 中的 offsetPy（全局）
            long currOffsetPy = slot.lastPos;
            // 延时消息与取消该延时消息的消息必须位于同一个 slot 中
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
            // slot 中正常的延时消息
            LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
            // 取消该 slot 中延时消息的消息
            LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();
            // 缓存 slot 中的延时消息在 timerlog 中的内容（SelectMappedBufferResult）
            LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
            SelectMappedBufferResult timeSbr = null;
            //read the timer log one by one
            // slot 中第一个延时消息的 prevPos = -1
            // 从 slot 中的 lastpos 开始从 timerlog 读取，一直读到 firstpos
            // 循环读取 slot 中所有延时消息
            while (currOffsetPy != -1) {
                perfCounterTicks.startTick("dequeue_read_timerlog");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    // 获取 currOffsetPy 所在 mappedFile，范围为 currOffsetPy 到 writePosition
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) {
                        sbrs.add(timeSbr);
                    }
                }
                if (null == timeSbr) {
                    break;
                }
                long prevPos = -1;
                try {
                    // currOffsetPy 为延时消息在 timerlog 中的全局 OffsetPy
                    // 获取 currOffsetPy 在对应 mappedFile 中的具体位置（局部）
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    // 前一个 延时消息, slot 中第一个延时消息的 prevPos = -1
                    prevPos = timeSbr.getByteBuffer().getLong();
                    int magic = timeSbr.getByteBuffer().getInt();
                    long enqueueTime = timeSbr.getByteBuffer().getLong();
                    long delayedTime = timeSbr.getByteBuffer().getInt() + enqueueTime;
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    // 从 timerlog 读取延时消息转换为 TimerRequest
                    TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic);
                    timerRequest.setDeleteList(deleteUniqKeys);
                    if (needDelete(magic) && !needRoll(magic)) {
                        // delete message
                        deleteMsgStack.add(timerRequest);
                    } else {
                        // normal or needRoll message
                        normalMsgStack.addFirst(timerRequest);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in dequeue_read_timerlog", e);
                } finally {
                    // 继续向前读取
                    currOffsetPy = prevPos;
                    perfCounterTicks.endTick("dequeue_read_timerlog");
                }
            }
            if (deleteMsgStack.size() == 0 && normalMsgStack.size() == 0) {
                LOGGER.warn("dequeue time:{} but read nothing from timerLog", currReadTimeMs);
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            // deleteMsg 个数
            CountDownLatch deleteLatch = new CountDownLatch(deleteMsgStack.size());
            //read the delete msg: the msg used to mark another msg is deleted
            // splitIntoLists： 收集位于同一 commitlog 文件的延时消息
            // 每个 list 中的延时消息均位于同一个 commitlog 文件中
            for (List<TimerRequest> deleteList : splitIntoLists(deleteMsgStack)) {
                // 每个 list 中的延时消息均位于同一个 commitlog 文件中
                for (TimerRequest tr : deleteList) {
                    tr.setLatch(deleteLatch);
                }
                // 将 deleteList 放入 dequeueGetQueue 中
                dequeueGetQueue.put(deleteList);
            }
            //do we need to use loop with tryAcquire
            // 等待对应的延时消息被取消删除
            checkDequeueLatch(deleteLatch, currReadTimeMs);

            CountDownLatch normalLatch = new CountDownLatch(normalMsgStack.size());
            //read the normal msg
            for (List<TimerRequest> normalList : splitIntoLists(normalMsgStack)) {
                for (TimerRequest tr : normalList) {
                    tr.setLatch(normalLatch);
                }
                // 每个 list 中的延时消息均位于同一个 commitlog 文件中
                dequeueGetQueue.put(normalList);
            }
            // 等待到期的延时消息被处理
            checkDequeueLatch(normalLatch, currReadTimeMs);
            // if master -> slave -> master, then the read time move forward, and messages will be lossed
            if (dequeueStatusChangeFlag) {
                return -1;
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            // currReadTimeMs 向前推进 precisionMs
            moveReadTime();
        } catch (Throwable t) {
            LOGGER.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                moveReadTime();
            }
        }
        return 1;
    }
    // 收集位于同一 commitlog 文件的延时消息
    // 每个 list 中的延时消息均位于同一个 commitlog 文件中
    private List<List<TimerRequest>> splitIntoLists(List<TimerRequest> origin) {
        //this method assume that the origin is not null;
        List<List<TimerRequest>> lists = new LinkedList<>();
        if (origin.size() < 100) {
            lists.add(origin);
            return lists;
        }
        // 收集位于同一 commitlog 文件的延时消息
        List<TimerRequest> currList = null;
        // 延时消息位于 commitlog 中的第几个 mappedFile
        int fileIndexPy = -1;
        int msgIndex = 0;
        for (TimerRequest tr : origin) {
            // OffsetPy : 延时消息在 commitlog 中的 offset
            if (fileIndexPy != tr.getOffsetPy() / commitLogFileSize) {
                // 与前一个延时消息位于不同的 commitlog 文件
                msgIndex = 0;
                if (null != currList && currList.size() > 0) {
                    // 将 currList 写入最终集合中
                    lists.add(currList);
                }
                // 重新开启一个新的 list, 存放位于同一 commitlog 文件的延时消息
                currList = new LinkedList<>();
                currList.add(tr);
                fileIndexPy = (int) (tr.getOffsetPy() / commitLogFileSize);
            } else {
                // 收集位于同一 commitlog 文件的延时消息
                currList.add(tr);
                if (++msgIndex % 2000 == 0) {
                    lists.add(currList);
                    // 每收集到 2000 个延时消息，则换一个 ArrayList 存储
                    currList = new ArrayList<>();
                }
            }
        }
        if (null != currList && currList.size() > 0) {
            lists.add(currList);
        }
        return lists;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        for (int i = 0; i < 3; i++) {
            MessageExt msgExt = null;
            bufferLocal.get().position(0);
            bufferLocal.get().limit(sizePy);
            boolean res = messageStore.getData(offsetPy, sizePy, bufferLocal.get());
            if (res) {
                bufferLocal.get().flip();
                msgExt = MessageDecoder.decode(bufferLocal.get(), true, false, false);
            }
            if (null == msgExt) {
                LOGGER.warn("Fail to read msg from commitLog offsetPy:{} sizePy:{}", offsetPy, sizePy);
            } else {
                return msgExt;
            }
        }
        return null;
    }
    // needRoll 则将消息重新投递到 TIMER_TOPIC 中，否则投递到 real topic 中
    public MessageExtBrokerInner convert(MessageExt messageExt, long enqueueTime, boolean needRoll) {
        if (enqueueTime != -1) {
            MessageAccessor.putProperty(messageExt, TIMER_ENQUEUE_MS, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, 1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        // needRoll 则将消息重新投递到 TIMER_TOPIC 中，否则投递到 real topic 中
        MessageExtBrokerInner message = convertMessage(messageExt, needRoll);
        return message;
    }

    //0 succ; 1 fail, need retry; 2 fail, do not retry;
    public int doPut(MessageExtBrokerInner message, boolean roll) throws Exception {

        if (!roll && null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            LOGGER.warn("Trying do put delete timer msg:[{}] roll:[{}]", message, roll);
            return PUT_NO_RETRY;
        }

        PutMessageResult putMessageResult = null;
        if (escapeBridgeHook != null) {
            putMessageResult = escapeBridgeHook.apply(message);
        } else {
            // 投递到期的延时消息到 real topic 中
            putMessageResult = messageStore.putMessage(message);
        }

        if (putMessageResult != null && putMessageResult.getPutMessageStatus() != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    if (brokerStatsManager != null) {
                        brokerStatsManager.incTopicPutNums(message.getTopic(), 1, 1);
                        if (putMessageResult.getAppendMessageResult() != null) {
                            brokerStatsManager.incTopicPutSize(message.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                        }
                        brokerStatsManager.incBrokerPutNums(message.getTopic(), 1);
                    }
                    return PUT_OK;

                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                case WHEEL_TIMER_NOT_ENABLE:
                case WHEEL_TIMER_MSG_ILLEGAL:
                    return PUT_NO_RETRY;

                case SERVICE_NOT_AVAILABLE:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case OS_PAGE_CACHE_BUSY:
                case CREATE_MAPPED_FILE_FAILED:
                case SLAVE_NOT_AVAILABLE:
                    return PUT_NEED_RETRY;

                case UNKNOWN_ERROR:
                default:
                    if (storeConfig.isTimerSkipUnknownError()) {
                        LOGGER.warn("Skipping message due to unknown error, msg: {}", message);
                        return PUT_NO_RETRY;
                    } else {
                        holdMomentForUnknownError();
                        return PUT_NEED_RETRY;
                    }
            }
        }
        return PUT_NEED_RETRY;
    }

    public MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, MessageAccessor.deepCopyProperties(msgExt.getProperties()));
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        // 不需要等待刷盘或者同步 slave 结果
        msgInner.setWaitStoreMsgOK(false);

        if (needRoll) {
            // 重新投递到 TIMER_TOPIC,当消息的延时时间超过 2 天的时候就需要 roll （设计有点像多级时间轮）
            // 防止因为延时时间太久，commitlog 的内容被删除
            // need roll message 的 deliver time 会被重新设置，如果原来的 deliver timer - 当前时间 - timerRollWindow(2天)
            // 这个增量没超过三分之一的 timerRollWindow，那么 need roll 消息的 deliver timer 会被设置为 当前时间 + 二分之一 timerRollWindow
            // 否则设置为 当前时间 + 二分之一 timerRollWindow
            // 当 need roll message 到期之后就会来到这里，重新被投递到 TIMER_TOPIC 中，重新走一遍整个延时调度流程
            // 会再次来到 need roll 的判断，将 origin deliver timer - 当前时间  看看是否超过 timerRollWindow
            // 没有超过则按照正常延时消息投递，如果超过则在走一遍 need roll 流程
            msgInner.setTopic(msgExt.getTopic());
            msgInner.setQueueId(msgExt.getQueueId());
        } else {
            // 投递到真正的 topic 中
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgInner.setQueueId(Integer.parseInt(msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        }
        return msgInner;
    }

    protected String getRealTopic(MessageExt msgExt) {
        if (msgExt == null) {
            return null;
        }
        return msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
    }

    private long formatTimeMs(long timeMs) {
        // 用于将 timeMs 向下取整到 precisionMs 的整数倍。它是一种常见的时间对齐或降精度技巧。
        // / 执行整数除法，结果截断小数部分
        // 周期性任务：将时间戳对齐到固定周期起点，如 1 秒、10 毫秒。
        // 时间索引：生成离散的时间桶（bucket）。
        return timeMs / precisionMs * precisionMs;
    }

    public int hashTopicForMetrics(String topic) {
        return null == topic ? 0 : topic.hashCode();
    }

    public void checkAndReviseMetrics() {
        Map<String, TimerMetrics.Metric> smallOnes = new HashMap<>();
        Map<String, TimerMetrics.Metric> bigOnes = new HashMap<>();
        Map<Integer, String> smallHashs = new HashMap<>();
        Set<Integer> smallHashCollisions = new HashSet<>();
        for (Map.Entry<String, TimerMetrics.Metric> entry : timerMetrics.getTimingCount().entrySet()) {
            if (entry.getValue().getCount().get() < storeConfig.getTimerMetricSmallThreshold()) {
                smallOnes.put(entry.getKey(), entry.getValue());
                int hash = hashTopicForMetrics(entry.getKey());
                if (smallHashs.containsKey(hash)) {
                    LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-small code:{} small topic:{}{} small topic:{}{}", hash,
                        entry.getKey(), entry.getValue(),
                        smallHashs.get(hash), smallOnes.get(smallHashs.get(hash)));
                    smallHashCollisions.add(hash);
                }
                smallHashs.put(hash, entry.getKey());
            } else {
                bigOnes.put(entry.getKey(), entry.getValue());
            }
        }
        //check the hash collision between small ons and big ons
        for (Map.Entry<String, TimerMetrics.Metric> bjgEntry : bigOnes.entrySet()) {
            if (smallHashs.containsKey(hashTopicForMetrics(bjgEntry.getKey()))) {
                Iterator<Map.Entry<String, TimerMetrics.Metric>> smallIt = smallOnes.entrySet().iterator();
                while (smallIt.hasNext()) {
                    Map.Entry<String, TimerMetrics.Metric> smallEntry = smallIt.next();
                    if (hashTopicForMetrics(smallEntry.getKey()) == hashTopicForMetrics(bjgEntry.getKey())) {
                        LOGGER.warn("[CheckAndReviseMetrics]Metric hash collision between small-big code:{} small topic:{}{} big topic:{}{}", hashTopicForMetrics(smallEntry.getKey()),
                            smallEntry.getKey(), smallEntry.getValue(),
                            bjgEntry.getKey(), bjgEntry.getValue());
                        smallIt.remove();
                    }
                }
            }
        }
        //refresh
        smallHashs.clear();
        Map<String, TimerMetrics.Metric> newSmallOnes = new HashMap<>();
        for (String topic : smallOnes.keySet()) {
            newSmallOnes.put(topic, new TimerMetrics.Metric());
            smallHashs.put(hashTopicForMetrics(topic), topic);
        }

        //travel the timer log
        long readTimeMs = currReadTimeMs;
        long currOffsetPy = timerWheel.checkPhyPos(readTimeMs, 0);
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        boolean hasError = false;
        try {
            while (true) {
                SelectMappedBufferResult timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                if (timeSbr == null) {
                    break;
                } else {
                    sbrs.add(timeSbr);
                }
                ByteBuffer bf = timeSbr.getByteBuffer();
                for (int position = 0; position < timeSbr.getSize(); position += TimerLog.UNIT_SIZE) {
                    bf.position(position);
                    bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt(); //magic
                    long enqueueTime = bf.getLong();
                    long delayedTime = bf.getInt() + enqueueTime;
                    long offsetPy = bf.getLong();
                    int sizePy = bf.getInt();
                    int hashCode = bf.getInt();
                    if (delayedTime < readTimeMs) {
                        continue;
                    }
                    if (!smallHashs.containsKey(hashCode)) {
                        continue;
                    }
                    String topic = null;
                    if (smallHashCollisions.contains(hashCode)) {
                        MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
                        if (null != messageExt) {
                            topic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);
                        }
                    } else {
                        topic = smallHashs.get(hashCode);
                    }
                    if (null != topic && newSmallOnes.containsKey(topic)) {
                        newSmallOnes.get(topic).getCount().addAndGet(needDelete(magic) ? -1 : 1);
                    } else {
                        LOGGER.warn("[CheckAndReviseMetrics]Unexpected topic in checking timer metrics topic:{} code:{} offsetPy:{} size:{}", topic, hashCode, offsetPy, sizePy);
                    }
                }
                if (timeSbr.getSize() < timerLogFileSize) {
                    break;
                } else {
                    currOffsetPy = currOffsetPy + timerLogFileSize;
                }
            }

        } catch (Exception e) {
            hasError = true;
            LOGGER.error("[CheckAndReviseMetrics]Unknown error in checkAndReviseMetrics and abort", e);
        } finally {
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) {
                    sbr.release();
                }
            }
        }

        if (!hasError) {
            //update
            for (String topic : newSmallOnes.keySet()) {
                LOGGER.info("[CheckAndReviseMetrics]Revise metric for topic {} from {} to {}", topic, smallOnes.get(topic), newSmallOnes.get(topic));
            }
            timerMetrics.getTimingCount().putAll(newSmallOnes);
        }

    }

    public class TimerEnqueueGetService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    // TIMER_TOPIC 只有一个 queue (queueId = 0)
                    // 从 TIMER_TOPIC queue 中不断的消费延时消息，封装 TimerRequest 并加入到 enqueuePutQueue
                    if (!TimerMessageStore.this.enqueue(0)) {
                        // 入队失败
                        // 每隔 十分之一时间精度(precisionMs) 执行 enqueue
                        waitForRunning(100L * precisionMs / 1000);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TimerMessageStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    public class TimerEnqueuePutService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        /**
         * collect the requests
         */
        protected List<TimerRequest> fetchTimerRequests() throws InterruptedException {
            List<TimerRequest> trs = null;
            TimerRequest firstReq = enqueuePutQueue.poll(10, TimeUnit.MILLISECONDS);
            if (null != firstReq) {
                trs = new ArrayList<>(16);
                trs.add(firstReq);
                while (true) {
                    TimerRequest tmpReq = enqueuePutQueue.poll(3, TimeUnit.MILLISECONDS);
                    if (null == tmpReq) {
                        break;
                    }
                    trs.add(tmpReq);
                    // 每次拉取 10个
                    if (trs.size() > 10) {
                        break;
                    }
                }
            }
            return trs;
        }

        protected void putMessageToTimerWheel(TimerRequest req) {
            try {
                perfCounterTicks.startTick(ENQUEUE_PUT);
                DefaultStoreMetricsManager.incTimerEnqueueCount(getRealTopic(req.getMsg()));
                // 如果延时消息现在已经到期，那么直接添加到 dequeuePutQueue，不会进入 TimerWheel
                if (shouldRunningDequeue && req.getDelayTime() < currWriteTimeMs) {
                    req.setEnqueueTime(Long.MAX_VALUE);
                    dequeuePutQueue.put(req);
                } else {
                    // 写入 timerlog ，更新 timerwheel 对应 slot 信息
                    boolean doEnqueueRes = doEnqueue(
                        req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                    // latch.countDown()
                    req.idempotentRelease(doEnqueueRes || storeConfig.isTimerSkipUnknownError());
                }
                perfCounterTicks.endTick(ENQUEUE_PUT);
            } catch (Throwable t) {
                LOGGER.error("Unknown error", t);
                if (storeConfig.isTimerSkipUnknownError()) {
                    req.idempotentRelease(true);
                } else {
                    holdMomentForUnknownError();
                }
            }
        }
        // 从 enqueuePutQueue 中拉取最多 10 个 TimerRequest
        // TimerRequest 写入 timerlog ，更新 timerwheel 对应 slot 信息
        // 更新 commitQueueOffset
        protected void fetchAndPutTimerRequest() throws Exception {
            // TIMER_TOPIC queue 的当前处理位置
            // 每当从 queue 中读取出一个延时消息并将其 enqueuePutQueue 之后这里就向前推进到下一个 offset
            long tmpCommitQueueOffset = currQueueOffset;
            // 从 enqueuePutQueue 中拉取最多 10 个 TimerRequest, 如果 enqueuePutQueue 中没有数据则返回 null
            List<TimerRequest> trs = this.fetchTimerRequests();
            if (CollectionUtils.isEmpty(trs)) {
                // enqueuePutQueue 中没有数据可拉取
                commitQueueOffset = tmpCommitQueueOffset;
                // 推进 currWriteTimeMs
                maybeMoveWriteTime();
                return;
            }

            while (!isStopped()) {
                // 等待被写入 TimerWheel
                CountDownLatch latch = new CountDownLatch(trs.size());
                for (TimerRequest req : trs) {
                    req.setLatch(latch);
                    // TimerRequest 写入 timerlog ，更新 timerwheel 对应 slot 信息
                    // latch.countDown()
                    this.putMessageToTimerWheel(req);
                }
                checkDequeueLatch(latch, -1);
                boolean allSuccess = trs.stream().allMatch(TimerRequest::isSucc);
                if (allSuccess) {
                    break;
                } else {
                    // sleep 50ms
                    holdMomentForUnknownError();
                }
            }
            // 最后一个写入 timerlog 的延时消息对应在 TIMER_TOPIC queue 中的 queueOffset
            commitQueueOffset = trs.get(trs.size() - 1).getMsg().getQueueOffset();
            maybeMoveWriteTime();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped() || enqueuePutQueue.size() != 0) {
                try {
                    // 从 enqueuePutQueue 中拉取最多 10 个 TimerRequest
                    // TimerRequest 写入 timerlog ，更新 timerwheel 对应 slot 信息
                    // 更新 commitQueueOffset
                    fetchAndPutTimerRequest();
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Unknown error", e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public class TimerDequeueGetService extends ServiceThread {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (System.currentTimeMillis() < shouldStartTime) {
                        TimerMessageStore.LOGGER.info("TimerDequeueGetService ready to run after {}.", shouldStartTime);
                        waitForRunning(1000);
                        continue;
                    }
                    // 将 currReadTimeMs 对应在 timer wheel 中的 slot 中的延时消息全部从 timerlog 中读取出来并封装成 TimerRequest
                    // 将这些 TimerRequest list 全部添加到 dequeueGetQueue 中等待处理，当 TimerRequest 全部被处理完成
                    // currReadTimeMs 向前推进 precisionMs
                    if (-1 == TimerMessageStore.this.dequeue()) {
                        // 每十分之一 precisionMs 间隔
                        waitForRunning(100L * precisionMs / 1000);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    abstract class AbstractStateService extends ServiceThread {
        public static final int INITIAL = -1, START = 0, WAITING = 1, RUNNING = 2, END = 3;
        protected int state = INITIAL;

        protected void setState(int state) {
            this.state = state;
        }

        protected boolean isState(int state) {
            return this.state == state;
        }
    }

    public class TimerDequeuePutMessageService extends AbstractStateService {
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }
        // 每隔 10 ms 从 dequeuePutQueue 中就到期的 TimerRequest 取出
        // 如果 TimerRequest 是正常的到期延时消息，则将其投递到 real topic 中
        // 如果是到期的 nedd roll 消息，则重新投递到 TIMER_TOPIC
        @Override
        public void run() {
            setState(AbstractStateService.START);
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            // dequeuePutQueue 中存储的均是已经到期的延时消息或者 needRoll 消息
            while (!this.isStopped() || dequeuePutQueue.size() != 0) {
                try {
                    setState(AbstractStateService.WAITING);
                    // 获取已经到期的延时消息或者 needRoll 消息
                    // 每隔 10 ms
                    TimerRequest tr = dequeuePutQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (null == tr) {
                        continue;
                    }

                    setState(AbstractStateService.RUNNING);
                    boolean tmpDequeueChangeFlag = false;

                    try {
                        while (!isStopped()) {
                            if (!isRunningDequeue()) {
                                dequeueStatusChangeFlag = true;
                                tmpDequeueChangeFlag = true;
                                break;
                            }

                            try {
                                perfCounterTicks.startTick(DEQUEUE_PUT);
                                // 延时消息在 commitlog 中的内容
                                MessageExt msgExt = tr.getMsg();
                                DefaultStoreMetricsManager.incTimerDequeueCount(getRealTopic(msgExt));
                                // 进入 enqueuePutQueue 的时间戳
                                if (tr.getEnqueueTime() == Long.MAX_VALUE) {
                                    // Never enqueue, mark it.
                                    MessageAccessor.putProperty(msgExt, TIMER_ENQUEUE_MS, String.valueOf(Long.MAX_VALUE));
                                }

                                addMetric(msgExt, -1);
                                // needRoll 则将消息重新投递到 TIMER_TOPIC 中，否则投递到 real topic 中
                                MessageExtBrokerInner msg = convert(msgExt, tr.getEnqueueTime(), needRoll(tr.getMagic()));

                                boolean processed = false;
                                int retryCount = 0;
                                // 不停的尝试投递
                                while (!processed && !isStopped()) {
                                    int result = doPut(msg, needRoll(tr.getMagic()));

                                    if (result == PUT_OK) {
                                        processed = true;
                                    } else if (result == PUT_NO_RETRY) {
                                        TimerMessageStore.LOGGER.warn("Skipping message due to unrecoverable error. Msg: {}", msg);
                                        processed = true;
                                    } else {
                                        retryCount++;
                                        // Without enabling TimerEnableRetryUntilSuccess, messages will retry up to 3 times before being discarded
                                        if (!storeConfig.isTimerEnableRetryUntilSuccess() && retryCount >= 3) {
                                            TimerMessageStore.LOGGER.error("Message processing failed after {} retries. Msg: {}", retryCount, msg);
                                            processed = true;
                                        } else {
                                            // 二分之一 precisionMs
                                            Thread.sleep(500L * precisionMs / 1000);
                                            TimerMessageStore.LOGGER.warn("Retrying to process message. Retry count: {}, Msg: {}", retryCount, msg);
                                        }
                                    }
                                }

                                perfCounterTicks.endTick(DEQUEUE_PUT);
                                break;

                            } catch (Throwable t) {
                                TimerMessageStore.LOGGER.info("Unknown error", t);
                                if (storeConfig.isTimerSkipUnknownError()) {
                                    break;
                                } else {
                                    holdMomentForUnknownError();
                                }
                            }
                        }
                    } finally {
                        // 将延时消息投递到 real topic 之后 release
                        tr.idempotentRelease(!tmpDequeueChangeFlag);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
            setState(AbstractStateService.END);
        }
    }

    public class TimerDequeueGetMessageService extends AbstractStateService {

        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }
        // 从 dequeueGetQueue.poll TimerRequest list(位于同一 commitlog 文件的到期延时消息或者 delete msgs)
        // 从 commitlog 中读取 TimerRequest 对应消息内容，如果是 delete msgs 则将 PROPERTY_TIMER_DEL_UNIQKEY
        // 写入到 TimerRequest 的 deleteList 中，一个 slot 中的所有 TimerRequest 共用一个 deleteList（需要被取消的延时消息）
        // 如果是 正常延时消息以及 needRoll 延时消息则判断消息是否存在 deleteList 中，如果存在则取消，不会将其放入 dequeuePutQueue 中
        // 否则进入 dequeuePutQueue
        @Override
        public void run() {
            setState(AbstractStateService.START);
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            //Mark different rounds
            boolean isRound = true;
            // 保存 delete msgs ,key 为 PROPERTY_TIMER_DEL_UNIQKEY， value: delete msg
            Map<String ,MessageExt> avoidDeleteLose = new HashMap<>(); // 栈中分配，因为作用范围不会逃逸出本方法外
            while (!this.isStopped()) {
                try {
                    // 等待拉取 dequeueGetQueue
                    setState(AbstractStateService.WAITING);
                    // 从 dequeueGetQueue 中拉取到期的延时消息（包括正常的延时消息以及 delete msgs）
                    // 队列元素 list 中保存的是位于同一 commitlog 文件的到期延时消息或者 delete msgs
                    // 每隔十分之一 precisionMs 进行拉取
                    List<TimerRequest> trs = dequeueGetQueue.poll(100L * precisionMs / 1000, TimeUnit.MILLISECONDS);
                    if (null == trs || trs.size() == 0) {
                        continue;
                    }
                    // 处理 dequeueGetQueue 中的到期 TimerRequest
                    setState(AbstractStateService.RUNNING);
                    // 位于同一 commitlog 文件的到期延时消息或者 delete msgs
                    for (int i = 0; i < trs.size(); ) {
                        TimerRequest tr = trs.get(i);
                        boolean doRes = false;
                        try {
                            long start = System.currentTimeMillis();
                            // 从 commitlog 中获取延时消息内容
                            MessageExt msgExt = getMessageByCommitOffset(tr.getOffsetPy(), tr.getSizePy());
                            if (null != msgExt) {
                                // 处理 delete msgs
                                if (needDelete(tr.getMagic()) && !needRoll(tr.getMagic())) {
                                    //Clearing is performed once in each round.
                                    //The deletion message is received first and the common message is received once
                                    // isRound 初始为 true
                                    if (!isRound) {
                                        isRound = true;
                                        for (MessageExt messageExt: avoidDeleteLose.values()) {
                                            addMetric(messageExt, 1);
                                        }
                                        avoidDeleteLose.clear();
                                    }
                                    // tr.getDeleteList() 为一个空的 ConcurrentSkipListSet，由 deQueue 方法设置
                                    // 要取消的延时消息 UNIQKEY
                                    if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null && tr.getDeleteList() != null) {
                                        // 保存 delete msgs ,key 为 PROPERTY_TIMER_DEL_UNIQKEY， value: delete msg
                                        avoidDeleteLose.put(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY), msgExt);
                                        // 正常延时消息以及 needRoll 延时消息这里的 tr.getDeleteList() 和前面 delete msg 中的 getDeleteList 是同一个实例 ConcurrentSkipListSet
                                        // 在 deQueue 方法中所有 TimerRequest 都会统一设置相同的 ConcurrentSkipListSet 实例，这里会往里面添加要取消的 PROPERTY_TIMER_DEL_UNIQKEY
                                        tr.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
                                    }
                                    // latch.countDown()
                                    tr.idempotentRelease();
                                    doRes = true;
                                } else {
                                    // 处理正常延时消息以及 needRoll 延时消息
                                    String uniqueKey = MessageClientIDSetter.getUniqID(msgExt);
                                    if (null == uniqueKey) {
                                        LOGGER.warn("No uniqueKey for msg:{}", msgExt);
                                    }
                                    //Mark ready for next round
                                    // isRound 初始为 true
                                    if (isRound) {
                                        isRound = false;
                                    }
                                    // 正常延时消息以及 needRoll 延时消息这里的 tr.getDeleteList() 和前面 delete msg 中的 getDeleteList 是同一个实例 ConcurrentSkipListSet
                                    // 但现在 ConcurrentSkipListSet 中已经保存了 PROPERTY_TIMER_DEL_UNIQKEY
                                    if (null != uniqueKey && tr.getDeleteList() != null && tr.getDeleteList().size() > 0
                                        && tr.getDeleteList().contains(buildDeleteKey(getRealTopic(msgExt), uniqueKey))) {
                                        avoidDeleteLose.remove(uniqueKey);
                                        doRes = true;
                                        // 正常延时消息被取消不会投递到 real topic 中
                                        tr.idempotentRelease();
                                        perfCounterTicks.getCounter("dequeue_delete").flow(1);
                                    } else {
                                        // 设置 commitlog 中的消息内容
                                        tr.setMsg(msgExt);
                                        // 每处理一个 TimerRequest 之前会设置 doRes = false
                                        while (!isStopped() && !doRes) {
                                            // 将到期延时消息或者 needRoll 延时消息添加到 dequeuePutQueue
                                            doRes = dequeuePutQueue.offer(tr, 3, TimeUnit.SECONDS);
                                        }
                                    }
                                }
                                perfCounterTicks.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
                            } else {
                                //the tr will never be processed afterwards, so idempotentRelease it
                                tr.idempotentRelease();
                                doRes = true;
                                perfCounterTicks.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);
                            }
                        } catch (Throwable e) {
                            LOGGER.error("Unknown exception", e);
                            if (storeConfig.isTimerSkipUnknownError()) {
                                tr.idempotentRelease();
                                doRes = true;
                            } else {
                                holdMomentForUnknownError();
                            }
                        } finally {
                            // 处理成功 -> 成功将 TimerRequest 添加到 dequeuePutQueue 或者成功取消了 TimerRequest
                            if (doRes) {
                                // 处理下一个 TimerRequest
                                i++;
                            }
                        }
                    }
                    // 清理 trs ，开始继续 dequeueGetQueue.poll
                    trs.clear();
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
            // TimerDequeueGetMessageService 停止运行
            setState(AbstractStateService.END);
        }
    }

    public class TimerDequeueWarmService extends ServiceThread {

        @Override
        public String getServiceName() {
            String brokerIdentifier = "";
            if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore && ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().getIdentifier();
            }
            return brokerIdentifier + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    //if (!storeConfig.isTimerWarmEnable() || -1 == TimerMessageStore.this.warmDequeue()) {
                    waitForRunning(50);
                    //}
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public boolean needRoll(int magic) {
        return (magic & MAGIC_ROLL) != 0;
    }

    public boolean needDelete(int magic) {
        return (magic & MAGIC_DELETE) != 0;
    }

    public class TimerFlushService extends ServiceThread {
        private final SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");

        @Override public String getServiceName() {
            String brokerIdentifier = "";
            if (TimerMessageStore.this.messageStore instanceof DefaultMessageStore && ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = ((DefaultMessageStore) TimerMessageStore.this.messageStore).getBrokerConfig().getIdentifier();
            }
            return brokerIdentifier + this.getClass().getSimpleName();
        }

        private String format(long time) {
            return sdf.format(new Date(time));
        }
        // 每隔 1000 ms 执行 flush timerLog,timerWheel,timerCheckpoint
        @Override
        public void run() {
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service start");
            long start = System.currentTimeMillis();
            while (!this.isStopped()) {
                try {
                    prepareTimerCheckPoint();
                    // 只要是 writePosition > flushPosition 就立即 flush
                    timerLog.getMappedFileQueue().flush(0);
                    // localBuffer 写入 mappedByteBuffer 然后 force
                    timerWheel.flush();
                    timerCheckpoint.flush();
                    // timerProgressLogIntervalMs = 10 * 1000
                    if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                        start = System.currentTimeMillis();
                        long tmpQueueOffset = currQueueOffset;
                        ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                        TimerMessageStore.LOGGER.info("[{}]Timer progress-check commitRead:[{}] currRead:[{}] currWrite:[{}] readBehind:{} currReadOffset:{} offsetBehind:{} behindMaster:{} " +
                                "enqPutQueue:{} deqGetQueue:{} deqPutQueue:{} allCongestNum:{} enqExpiredStoreTime:{}",
                            storeConfig.getBrokerRole(),
                            format(commitReadTimeMs), format(currReadTimeMs), format(currWriteTimeMs), getDequeueBehind(),
                            tmpQueueOffset, maxOffsetInQueue - tmpQueueOffset, timerCheckpoint.getMasterTimerQueueOffset() - tmpQueueOffset,
                            enqueuePutQueue.size(), dequeueGetQueue.size(), dequeuePutQueue.size(), getAllCongestNum(), format(lastEnqueueButExpiredStoreTime));
                    }
                    timerMetrics.persist();
                    // 每隔 1000 ms 执行 timer flush
                    waitForRunning(storeConfig.getTimerFlushIntervalMs());
                } catch (Throwable e) {
                    TimerMessageStore.LOGGER.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.LOGGER.info(this.getServiceName() + " service end");
        }
    }

    public long getAllCongestNum() {
        return timerWheel.getAllNum(currReadTimeMs);
    }

    public long getCongestNum(long deliverTimeMs) {
        return timerWheel.getNum(deliverTimeMs);
    }

    public boolean isReject(long deliverTimeMs) {
        long congestNum = timerWheel.getNum(deliverTimeMs);
        if (congestNum <= storeConfig.getTimerCongestNumEachSlot()) {
            return false;
        }
        if (congestNum >= storeConfig.getTimerCongestNumEachSlot() * 2L) {
            return true;
        }
        if (RANDOM.nextInt(1000) > 1000 * (congestNum - storeConfig.getTimerCongestNumEachSlot()) / (storeConfig.getTimerCongestNumEachSlot() + 0.1)) {
            return true;
        }
        return false;
    }

    public long getEnqueueBehindMessages() {
        long tmpQueueOffset = currQueueOffset;
        ConsumeQueueInterface cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
        return maxOffsetInQueue - tmpQueueOffset;
    }

    public long getEnqueueBehindMillis() {
        if (System.currentTimeMillis() - lastEnqueueButExpiredTime < 2000) {
            return System.currentTimeMillis() - lastEnqueueButExpiredStoreTime;
        }
        return 0;
    }

    public long getEnqueueBehind() {
        return getEnqueueBehindMillis() / 1000;
    }

    public long getDequeueBehindMessages() {
        return timerWheel.getAllNum(currReadTimeMs);
    }

    public long getDequeueBehindMillis() {
        return System.currentTimeMillis() - currReadTimeMs;
    }

    public long getDequeueBehind() {
        return getDequeueBehindMillis() / 1000;
    }

    public float getEnqueueTps() {
        return perfCounterTicks.getCounter(ENQUEUE_PUT).getLastTps();
    }

    public float getDequeueTps() {
        return perfCounterTicks.getCounter("dequeue_put").getLastTps();
    }

    public void prepareTimerCheckPoint() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
        // True if current store is master or current brokerId is equal to the minimum brokerId of the replica group in slaveActingMaster mode.
        if (shouldRunningDequeue) {
            timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
            if (commitReadTimeMs != lastCommitReadTimeMs || commitQueueOffset != lastCommitQueueOffset) {
                timerCheckpoint.updateDateVersion(messageStore.getStateMachineVersion());
                lastCommitReadTimeMs = commitReadTimeMs;
                lastCommitQueueOffset = commitQueueOffset;
            }
        }
        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
    }

    public void registerEscapeBridgeHook(Function<MessageExtBrokerInner, PutMessageResult> escapeBridgeHook) {
        this.escapeBridgeHook = escapeBridgeHook;
    }

    public boolean isMaster() {
        return BrokerRole.SLAVE != lastBrokerRole;
    }

    public long getCurrReadTimeMs() {
        return this.currReadTimeMs;
    }

    public long getQueueOffset() {
        return currQueueOffset;
    }

    public long getCommitQueueOffset() {
        return this.commitQueueOffset;
    }

    public long getCommitReadTimeMs() {
        return this.commitReadTimeMs;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerWheel getTimerWheel() {
        return timerWheel;
    }

    public TimerLog getTimerLog() {
        return timerLog;
    }

    public TimerMetrics getTimerMetrics() {
        return this.timerMetrics;
    }

    public int getPrecisionMs() {
        return precisionMs;
    }

    public TimerEnqueueGetService getEnqueueGetService() {
        return enqueueGetService;
    }

    public void setEnqueueGetService(TimerEnqueueGetService enqueueGetService) {
        this.enqueueGetService = enqueueGetService;
    }

    public TimerEnqueuePutService getEnqueuePutService() {
        return enqueuePutService;
    }

    public void setEnqueuePutService(TimerEnqueuePutService enqueuePutService) {
        this.enqueuePutService = enqueuePutService;
    }

    public TimerDequeueWarmService getDequeueWarmService() {
        return dequeueWarmService;
    }

    public void setDequeueWarmService(
        TimerDequeueWarmService dequeueWarmService) {
        this.dequeueWarmService = dequeueWarmService;
    }

    public TimerDequeueGetService getDequeueGetService() {
        return dequeueGetService;
    }

    public void setDequeueGetService(TimerDequeueGetService dequeueGetService) {
        this.dequeueGetService = dequeueGetService;
    }

    public TimerDequeuePutMessageService[] getDequeuePutMessageServices() {
        return dequeuePutMessageServices;
    }

    public void setDequeuePutMessageServices(
        TimerDequeuePutMessageService[] dequeuePutMessageServices) {
        this.dequeuePutMessageServices = dequeuePutMessageServices;
    }

    public TimerDequeueGetMessageService[] getDequeueGetMessageServices() {
        return dequeueGetMessageServices;
    }

    public void setDequeueGetMessageServices(
        TimerDequeueGetMessageService[] dequeueGetMessageServices) {
        this.dequeueGetMessageServices = dequeueGetMessageServices;
    }

    public void setTimerMetrics(TimerMetrics timerMetrics) {
        this.timerMetrics = timerMetrics;
    }

    public AtomicInteger getFrequency() {
        return frequency;
    }

    public void setFrequency(AtomicInteger frequency) {
        this.frequency = frequency;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return timerCheckpoint;
    }

    // identify a message by topic + uk, like query operation
    public static String buildDeleteKey(String realTopic, String uniqueKey) {
        return realTopic + "+" + uniqueKey;
    }
}

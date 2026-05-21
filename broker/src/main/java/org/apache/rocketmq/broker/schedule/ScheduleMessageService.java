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
package org.apache.rocketmq.broker.schedule;

import io.opentelemetry.api.common.Attributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_MESSAGE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
// 延时消息的核心实现

public class ScheduleMessageService extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private static final long DELAY_FOR_A_SLEEP = 10L;
    // delayLevel -> 延时时长
    // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    // 18 级
    private final ConcurrentSkipListMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentSkipListMap<>();
    // 加载自 storePath/config/delayOffset.json 文件
    // 缓存管理延时消息的投递进度
    // 按照 delayLevel 分类，delayLevel 可以看做是 RMQ_SYS_SCHEDULE_TOPIC 下的一个 queue
    // 一个 delayLevel 对应一个 queue, 每个 queue 中的延时消息由 deliverExecutorService 中的一个线程负责投递
    // 投递成功一个 ，这里的 queue offset 先前推进一个，offset 指示的是当前需要投递的延时消息，延时到期就投递，投递成功之后 offset 向前推进
    // 延时未到期或者投递失败，offset 不变
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<>(32);
    private final AtomicBoolean started = new AtomicBoolean(false);
    // 线程数对应 maxDelayLevel（18） ，一个 delayLevel 对应一个线程
    // 有界 DelayedWorkQueue 初始容量 16 （JDK 默认），注意这里的 queue 保存的是所有 delayLevel 的延时消息
    // 每个 delayLevel 队首的延时消息，如果还未到期就会被重新投递到 DelayedWorkQueue 中
    // 防止阻塞 deliverExecutorService 线程
    private ScheduledExecutorService deliverExecutorService;
    // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    // 18 级
    private int maxDelayLevel;
    private DataVersion dataVersion = new DataVersion();
    // 默认 false
    private boolean enableAsyncDeliver = false;
    private ScheduledExecutorService handleExecutorService;
    private final ScheduledExecutorService scheduledPersistService;
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);
    private final BrokerController brokerController;
    private final transient AtomicLong versionChangeCounter = new AtomicLong(0);

    public ScheduleMessageService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.enableAsyncDeliver = brokerController.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        scheduledPersistService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("ScheduleMessageServicePersistThread", true, brokerController.getBrokerConfig()));
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void buildRunningStats(HashMap<String, String> stats) throws ConsumeQueueException {
        for (Map.Entry<Integer, Long> next : this.offsetTable.entrySet()) {
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getDelayOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.load();
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            // 18 级
            // 线程数对应 maxDelayLevel ，一个 delayLevel 对应一个线程
            this.deliverExecutorService = ThreadUtils.newScheduledThreadPool(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
            if (this.enableAsyncDeliver) {
                this.handleExecutorService = ThreadUtils.newScheduledThreadPool(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
            }
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    // false
                    if (this.enableAsyncDeliver) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                    // 订阅 RMQ_SYS_SCHEDULE_TOPIC， 该 topic 下共有 18 个队列对应 18级 DelayLevel
                    // 每个线程负责处理一个 DelayLevel（可以看做是 RMQ_SYS_SCHEDULE_TOPIC 下的一个 queue）
                    // 如果 queue 中消息已经达到投递时间，那么就写入commitlog
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
            }

            scheduledPersistService.scheduleAtFixedRate(() -> {
                try {
                    ScheduleMessageService.this.persist();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }, 10000, this.brokerController.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        stop();
        ThreadUtils.shutdown(scheduledPersistService);
    }

    public boolean stop() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
            try {
                this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("deliverExecutorService awaitTermination error", e);
            }

            if (this.handleExecutorService != null) {
                this.handleExecutorService.shutdown();
                try {
                    this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("handleExecutorService awaitTermination error", e);
                }
            }

            for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
                log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
            }

            this.persist();
        }
        return true;
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        // storePath/config/delayOffset.json 文件
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        result = result && this.correctDelayOffset();
        return result;
    }

    public boolean loadWhenSyncDelayOffset() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            for (int delayLevel : delayLevelTable.keySet()) {
                ConsumeQueueInterface cq =
                    brokerController.getMessageStore().getQueueStore().findOrCreateConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController.getMessageStore().getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
                // For compatible
                if (delayOffsetSerializeWrapper.getDataVersion() != null) {
                    this.dataVersion.assignNewOne(delayOffsetSerializeWrapper.getDataVersion());
                }
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        delayOffsetSerializeWrapper.setDataVersion(this.dataVersion);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
        // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        // 18 级
        String levelString = this.brokerController.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parse message delay level failed. messageDelayLevel = {}", levelString, e);
            return false;
        }

        return true;
    }

    private MessageExtBrokerInner messageTimeUp(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

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

        msgInner.setWaitStoreMsgOK(false);
        // 清除延时消息属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELAY_SEC);
        // 还原 origin topic
        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
        // 还原 origin topic queue
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    class DeliverDelayedMessageTimerTask implements Runnable {
        // 可以看做 RMQ_SYS_SCHEDULE_TOPIC 中的 queueId
        private final int delayLevel;
        // queueId 对应的 offset
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    // 每个线程负责处理一个 DelayLevel（可以看做是 RMQ_SYS_SCHEDULE_TOPIC 下的一个 queue）
                    // 如果 queue 中消息已经达到投递时间，那么就写入commitlog
                    // 每个 delayLevel 队首的延时消息，如果还未到期就会被重新投递到 DelayedWorkQueue 中
                    // 防止阻塞 deliverExecutorService 线程
                    this.executeOnTimeUp();
                }
            } catch (Throwable e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeUp exception", e);
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
            }
        }

        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeUp() {
            // 获取 delayLevel 对应的 queue (RMQ_SYS_SCHEDULE_TOPIC)
            ConsumeQueueInterface cq =
                ScheduleMessageService.this.brokerController.getMessageStore().getConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            if (cq == null) {
                // DELAY_FOR_A_WHILE = 100L
                // 延时 100ms 在来拉取 delayLevel 对应的 queue
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }
            // 从 offset 处开始获取延时消息索引
            ReferredIterator<CqUnit> bufferCQ = cq.iterateFrom(this.offset);
            if (bufferCQ == null) {
                long resetOffset;
                if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else {
                    resetOffset = this.offset;
                }
                // 延时 100ms 从新的 resetOffset 处开始拉取 delayLevel 对应的 queue
                this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
                return;
            }

            long nextOffset = this.offset;
            try {
                while (bufferCQ.hasNext() && isStarted()) {
                    CqUnit cqUnit = bufferCQ.next();
                    long offsetPy = cqUnit.getPos();
                    int sizePy = cqUnit.getSize();
                    long tagsCode = cqUnit.getTagsCode();

                    if (!cqUnit.isTagsCodeValid()) {
                        //can't find ext content.So re compute tags code.
                        log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                            tagsCode, offsetPy, sizePy);
                        long msgStoreTime = ScheduleMessageService.this.brokerController.getMessageStore().getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                        tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                    }

                    long now = System.currentTimeMillis();
                    // 消息存储时间 + 延时时间（delayLevel）
                    long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                    long currOffset = cqUnit.getQueueOffset();
                    assert cqUnit.getBatchNum() == 1;
                    // 下一个延时消息
                    nextOffset = currOffset + cqUnit.getBatchNum();

                    long countdown = deliverTimestamp - now;
                    if (countdown > 0) {
                        // 如果 currOffset 延时消息还未到期，那么就等 100ms 再来查看是否到期，注意这里不会阻塞线程
                        // 每个 delayLevel 队首的延时消息，如果还未到期就会被重新投递到 DelayedWorkQueue 中
                        // 防止阻塞 deliverExecutorService 线程
                        this.scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
                        // 由于延时消息没有投递，所有进度依然是 currOffset
                        ScheduleMessageService.this.updateOffset(this.delayLevel, currOffset);
                        return;
                    }
                    // currOffset 延时消息到期
                    // 从 commitlog 中获取延时消息，注意这里的 topic 是 RMQ_SYS_SCHEDULE_TOPIC 而不是 origin topic
                    MessageExt msgExt = ScheduleMessageService.this.brokerController.getMessageStore().lookMessageByOffset(offsetPy, sizePy);
                    if (msgExt == null) {
                        continue;
                    }
                    // currOffset 延时消息到期
                    // 还原 origin topic 以及 origin queue
                    MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeUp(msgExt);
                    if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                            msgInner.getTopic(), msgInner);
                        continue;
                    }

                    boolean deliverSuc;
                    if (ScheduleMessageService.this.enableAsyncDeliver) {
                        deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                    } else {
                        // 发送到 commitlog, 投递到期的延时消息
                        // 延时消息投递进度向前推进到 NextOffset
                        deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                    }

                    if (!deliverSuc) {
                        // 投递失败还原投递进度
                        this.scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                }
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeUp execute error, offset = {}", nextOffset, e);
            } finally {
                bufferCQ.release();
            }

            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            // 发送到 commitlog
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
            PutMessageResult result = resultProcess.get();
            boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
            if (sendStatus) {
                // 延时消息投递进度向前推进到 NextOffset
                ScheduleMessageService.this.updateOffset(this.delayLevel, resultProcess.getNextOffset());
            }
            return sendStatus;
        }

        private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            Queue<PutResultProcess> processesQueue = ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            //Flow Control
            int currentPendingNum = processesQueue.size();
            int maxPendingLimit = brokerController.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxPendingLimit();
            if (currentPendingNum > maxPendingLimit) {
                log.warn("Asynchronous deliver triggers flow control, " +
                    "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                return false;
            }

            //Blocked
            PutResultProcess firstProcess = processesQueue.peek();
            if (firstProcess != null && firstProcess.need2Blocked()) {
                log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
                return false;
            }

            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
            processesQueue.add(resultProcess);
            return true;
        }

        private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
            long offsetPy, int sizePy, boolean autoResend) {
            CompletableFuture<PutMessageResult> future =
                brokerController.getEscapeBridge().asyncPutMessage(msgInner);
            return new PutResultProcess()
                .setTopic(msgInner.getTopic())
                .setDelayLevel(this.delayLevel)
                .setOffset(offset)
                .setPhysicOffset(offsetPy)
                .setPhysicSize(sizePy)
                .setMsgId(msgId)
                .setAutoResend(autoResend)
                .setFuture(future)
                .thenProcess();
        }
    }

    public class HandlePutResultTask implements Runnable {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    switch (putResultProcess.getStatus()) {
                        case SUCCESS:
                            ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                            pendingQueue.remove();
                            break;
                        case RUNNING:
                            scheduleNextTask();
                            return;
                        case EXCEPTION:
                            if (!isStarted()) {
                                log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                                return;
                            }
                            log.warn("putResultProcess error, info={}", putResultProcess.toString());
                            putResultProcess.doResend();
                            break;
                        case SKIP:
                            log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                            pendingQueue.remove();
                            break;
                    }
                } catch (Exception e) {
                    log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                    putResultProcess.doResend();
                }
            }

            scheduleNextTask();
        }

        private void scheduleNextTask() {
            if (isStarted()) {
                ScheduleMessageService.this.handleExecutorService
                    .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
            }
        }
    }

    public class PutResultProcess {
        private String topic;
        private long offset;
        private long physicOffset;
        private int physicSize;
        private int delayLevel;
        private String msgId;
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;

        private volatile AtomicInteger resendCount = new AtomicInteger(0);
        private volatile ProcessStatus status = ProcessStatus.RUNNING;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public AtomicInteger getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            this.future.thenAccept(this::handleResult);

            this.future.exceptionally(e -> {
                log.error("ScheduleMessageService put message exceptionally, info: {}",
                    PutResultProcess.this.toString(), e);

                onException();
                return null;
            });
            return this;
        }

        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                log.warn("ScheduleMessageService put message failed. info: {}.", result);
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.status = ProcessStatus.SUCCESS;
            if (ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig().isEnableScheduleMessageStats() && !result.isRemotePut()) {
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());

                Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                    .put(LABEL_TOPIC, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    .put(LABEL_CONSUMER_GROUP, MixAll.SCHEDULE_CONSUMER_GROUP)
                    .put(LABEL_IS_SYSTEM, true)
                    .build();
                BrokerMetricsManager.messagesOutTotal.add(result.getAppendMessageResult().getMsgNum(), attributes);
                BrokerMetricsManager.throughputOutTotal.add(result.getAppendMessageResult().getWroteBytes(), attributes);

                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incBrokerPutNums(this.topic, result.getAppendMessageResult().getMsgNum());

                attributes = BrokerMetricsManager.newAttributesBuilder()
                    .put(LABEL_TOPIC, topic)
                    .put(LABEL_MESSAGE_TYPE, TopicMessageType.DELAY.getMetricsValue())
                    .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic))
                    .build();
                BrokerMetricsManager.messagesInTotal.add(result.getAppendMessageResult().getMsgNum(), attributes);
                BrokerMetricsManager.throughputInTotal.add(result.getAppendMessageResult().getWroteBytes(), attributes);
                BrokerMetricsManager.messageSize.record(result.getAppendMessageResult().getWroteBytes() / result.getAppendMessageResult().getMsgNum(), attributes);
            }
        }

        public void onException() {
            log.warn("ScheduleMessageService onException, info: {}", this.toString());
            if (this.autoResend) {
                this.status = ProcessStatus.EXCEPTION;
            } else {
                this.status = ProcessStatus.SKIP;
            }
        }

        public ProcessStatus getStatus() {
            return this.status;
        }

        public PutMessageResult get() {
            try {
                return this.future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        public void doResend() {
            log.info("Resend message, info: {}", this.toString());

            // Gradually increase the resend interval.
            try {
                Thread.sleep(Math.min(this.resendCount.incrementAndGet() * 100, 60 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                MessageExt msgExt = ScheduleMessageService.this.brokerController.getMessageStore().lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                    this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                    return;
                }

                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeUp(msgExt);
                PutMessageResult result = ScheduleMessageService.this.brokerController.getEscapeBridge().putMessage(msgInner);
                this.handleResult(result);
                if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    log.info("Resend message success, info: {}", this.toString());
                }
            } catch (Exception e) {
                this.status = ProcessStatus.EXCEPTION;
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        public boolean need2Blocked() {
            int maxResendNum2Blocked = ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount.get() > maxResendNum2Blocked;
        }

        public boolean need2Skip() {
            int maxResendNum2Blocked = ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount.get() > maxResendNum2Blocked * 2;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                "topic='" + topic + '\'' +
                ", offset=" + offset +
                ", physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", delayLevel=" + delayLevel +
                ", msgId='" + msgId + '\'' +
                ", autoResend=" + autoResend +
                ", resendCount=" + resendCount +
                ", status=" + status +
                '}';
        }
    }

    public enum ProcessStatus {
        /**
         * In process, the processing result has not yet been returned.
         */
        RUNNING,

        /**
         * Put message success.
         */
        SUCCESS,

        /**
         * Put message exception. When autoResend is true, the message will be resend.
         */
        EXCEPTION,

        /**
         * Skip put message. When the message cannot be looked, the message will be skipped.
         */
        SKIP,
    }

    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }
}

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

package org.apache.rocketmq.broker.longpolling;

import java.util.Map;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.store.MessageArrivingListener;

public class NotifyMessageArrivingListener implements MessageArrivingListener {
    private final PullRequestHoldService pullRequestHoldService;
    private final PopMessageProcessor popMessageProcessor;
    private final NotificationProcessor notificationProcessor;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService, final PopMessageProcessor popMessageProcessor, final NotificationProcessor notificationProcessor) {
        this.pullRequestHoldService = pullRequestHoldService;
        this.popMessageProcessor = popMessageProcessor;
        this.notificationProcessor = notificationProcessor;
    }
    // logicOffset 为消息在对应 queueId 中的个数（并不是 bytes 单位），而是 queue 中的第几个消息
    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                         long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        // 通知 pullRequestHoldService 中正在等待 topic@queueId 消息的所有客户端 PullRequest
        // 根据消费端指定的 PullRequest （PullFromThisOffset）进行触发
        this.pullRequestHoldService.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        // 通知 popLongPollingService 中正在等待 topic@consumeGroup@queueId 消息的所有客户端 PopRequest
        // 只要 PopRequest 超时就触发，当然了前提都是消息到来的时候
        this.popMessageProcessor.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        // 逻辑同上
        this.notificationProcessor.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}

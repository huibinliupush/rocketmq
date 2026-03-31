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


import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;

// 作用同 groupTransferService
// FlushDiskWatcher 用于检测消息写入 commitlog 之后，如果是同步刷盘策略，就需要等待刷盘结果，刷盘超时通知超时，成功通知 PUT_OK
// groupTransferService 用于检测消息写入 commitlog 之后，同步到 slave 的个数，根据 HA 需要，可以选择同步 1 个或者多个 slave
public class FlushDiskWatcher extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 等待 commitRequests 中的 GroupCommitRequest 过期，然后调用 wakeupCustomer -> flushOKFuture.complete(FLUSH_DISK_TIMEOUT)
    // GroupCommitRequest 表示客户端等待刷盘的结果，其中包含一个 flushOKFuture
    // 在每次客户端向 commitlog 写入消息之后，都会创建一个 GroupCommitRequest，然后获取 flushOKFuture 等待消息刷盘结果
    // see : org.apache.rocketmq.store.CommitLog#asyncPutMessage 最后
    private final LinkedBlockingQueue<GroupCommitRequest> commitRequests = new LinkedBlockingQueue<>();

    @Override
    public String getServiceName() {
        return FlushDiskWatcher.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!isStopped()) {
            GroupCommitRequest request = null;
            try {
                // 等待 GroupCommitRequest 过期，然后调用 wakeupCustomer -> flushOKFuture.complete(FLUSH_DISK_TIMEOUT)
                request = commitRequests.take();
            } catch (InterruptedException e) {
                log.warn("take flush disk commit request, but interrupted, this may caused by shutdown");
                continue;
            }
            // GroupCommitService(同步刷盘策略下的 flushCommitlogService) 在 flush commitlog 之后
            // 会判断当前 commit log 的 flushWhere 是否超过了 request 的 nextOffset，如果超过了说明
            // 写入的 commitLog 的消息已经被flush 到磁盘了，然后通知 request.future complete with put_ok
            while (!request.future().isDone()) {
                long now = System.nanoTime();
                if (now - request.getDeadLine() >= 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                // To avoid frequent thread switching, replace future.get with sleep here,
                long sleepTime = (request.getDeadLine() - now) / 1_000_000; // 转换为ms
                // 如果 request 没有过期就等待它过期
                sleepTime = Math.min(10, sleepTime);
                if (sleepTime == 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                try {
                    // 等待 GroupCommitRequest 过期
                    // 过期之后再判断一下是否完成
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.warn(
                            "An exception occurred while waiting for flushing disk to complete. this may caused by shutdown");
                    break;
                }
            }
        }
    }

    public void add(GroupCommitRequest request) {
        commitRequests.add(request);
    }

    public int queueSize() {
        return commitRequests.size();
    }
}

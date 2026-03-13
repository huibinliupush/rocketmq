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
package org.apache.rocketmq.controller.impl.closure;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.concurrent.CompletableFuture;

public class ControllerClosure implements Closure {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final RemotingCommand requestEvent;
    private final CompletableFuture<RemotingCommand> future;
    // 状态机处理之后的结果
    private ControllerResult<?> controllerResult;
    // 用于向 raft 状态机提交任务
    private Task task;

    public ControllerClosure(RemotingCommand requestEvent) {
        // 存储的目的是为了让本机 raft 节点的状态机直接获取，不需要序列化
        // org.apache.rocketmq.controller.impl.JRaftControllerStateMachine.processEvent
        this.requestEvent = requestEvent;
        // task 提交到 raft 之后，直接向客户端返回该 future
        this.future = new CompletableFuture<>();
        this.task = null;
    }

    public CompletableFuture<RemotingCommand> getFuture() {
        return future;
    }

    public void setControllerResult(ControllerResult<?> controllerResult) {
        // 用于设置 raft 状态机的 apply 结果
        // 首先将请求提交到状态机 apply 方法，执行处理相应的 request
        // 结果由 raft 状态机设置到这里
        this.controllerResult = controllerResult;
    }

    @Override
    public void run(Status status) {
        // raft 状态机处理完 task 会调用这里
        if (status.isOk()) {
            final RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(controllerResult.getResponseCode(), (CommandCustomHeader) controllerResult.getResponse());
            if (controllerResult.getBody() != null) {
                response.setBody(controllerResult.getBody());
            }
            if (controllerResult.getRemark() != null) {
                response.setRemark(controllerResult.getRemark());
            }
            // 通知客户端
            future.complete(response);
        } else {
            log.error("Failed to append to jRaft node, error is: {}.", status);
            future.complete(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, status.getErrorMsg()));
        }
    }

    public Task taskWithThisClosure() {
        if (task != null) {
            return task;
        }
        task = new Task();
        // Closure done 任务的回调，在任务完成的时候通知此对象，无论成功还是失败。
        // 这个 closure 将在 StateMachine#onApply(iterator) 方法应用到状态机的时候，可以拿到并调用，一般用于客户端应答的返回。
        task.setDone(this);
        // encode request , 其他 raft 节点状态机在处理 request 的时候需要从 task data 中反序列化
        // org.apache.rocketmq.controller.impl.JRaftControllerStateMachine.processEvent
        task.setData(requestEvent.encode());
        return task;
    }

    public RemotingCommand getRequestEvent() {
        return requestEvent;
    }
}

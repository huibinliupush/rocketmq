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
package org.apache.rocketmq.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.annotation.JSONField;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;

import static org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE;

public class TopicConfig {
    private static final String SEPARATOR = " ";
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };
    private String topicName;
    // admin 创建默认为 8
    private int readQueueNums = defaultReadQueueNums;
    /**
     * writeQueueNums：指定生产者向该 Topic 发送消息时，可以写入的队列数量
     * readQueueNums：指定消费者从该 Topic 拉取消息时，可以拉取的队列数量
     *
     * 核心差异：一个是“物理存在”，一个是“逻辑视图”
     * 理解这两者最关键的在于物理存储层面：只有 writeQueueNums 指定的队列会实际创建对应的 ConsumeQueue 文件，而 readQueueNums 只是一个逻辑概念，用于告诉消费者可以从哪些队列中拉取消息
     * 两者相等（最佳实践）：每个物理队列都有对应的消费者，系统工作正常，资源利用率最高。
     *
     * writeQueueNums > readQueueNums（消息堆积，无法消费）：
     *
     * 写入：生产者向所有（例如 queue 0-7）物理队列写入数据。
     *
     * 消费：消费者只被允许从部分（例如 queue 0-3）队列拉取消息。
     *
     * 后果：写入的部分队列（queue 4-7）会持续堆积消息，永远无法被消费，造成严重积压。
     *
     * writeQueueNums < readQueueNums（消费者空闲，资源浪费）：
     *
     * 写入：生产者只向部分（例如 queue 0-3）物理队列写入数据。
     *
     * 消费：消费者组会尝试从更多（例如 queue 0-7）队列拉取消息。
     *
     * 后果：多出的消费者（拉取 queue 4-7 的）会因为对应队列中没有消息而闲置，造成资源浪费
     *
     * 设计初衷：实现平滑的队列缩容
     *
     * RocketMQ 之所以提供两个独立的参数，而不是直接合并为一个，是为了在不重启应用且不丢失消息的情况下，平滑地减少队列数量（缩容）。
     *
     * 假设要将一个 Topic 的队列数从 16 缩容到 8，如果直接同时修改 writeQueueNums 和 readQueueNums，可能会导致部分消息未被消费。而利用读写队列的分离设计，可以采用更安全的两步式操作：
     *
     * 第一步：缩容写队列。将 writeQueueNums 从 16 改为 8。此时，生产者只向 queue 0-7 写入新消息，而 queue 8-15 中已有的旧消息则不再有新消息写入。
     *
     * 第二步：等待消费完成。现有的消费者组继续消费，直到 queue 8-15 中的所有积压消息被完全处理完毕。
     *
     * 第三步：缩容读队列。将 readQueueNums 也从 16 改为 8。此时，消费者也将只从 queue 0-7 拉取消息，整个缩容过程平滑完成。
     *
     * 这种设计确保了队列缩容过程中没有消息丢失，也无需重启任何服务。
     * */
    private int writeQueueNums = defaultWriteQueueNums;
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    // org.apache.rocketmq.common.sysflag.TopicSysFlag.buildSysFlag
    // isUnit , isUNIT_SUB(isCenterSync)
    private int topicSysFlag = 0;
    private boolean order = false;
    // Field attributes should not have ' ' char in key or value, otherwise will lead to decode failure.
    private Map<String, String> attributes = new HashMap<>();

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm, int topicSysFlag) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
        this.topicSysFlag = topicSysFlag;
    }

    public TopicConfig(TopicConfig other) {
        this.topicName = other.topicName;
        this.readQueueNums = other.readQueueNums;
        this.writeQueueNums = other.writeQueueNums;
        this.perm = other.perm;
        this.topicFilterType = other.topicFilterType;
        this.topicSysFlag = other.topicSysFlag;
        this.order = other.order;
        this.attributes = other.attributes;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        //[0]
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        //[1]
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        //[2]
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        //[3]
        sb.append(this.perm);
        sb.append(SEPARATOR);
        //[4]
        sb.append(this.topicFilterType);
        sb.append(SEPARATOR);
        //[5]
        if (attributes != null) {
            sb.append(JSON.toJSONString(attributes));
        }

        return sb.toString();
    }

    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs.length >= 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            if (strs.length >= 6) {
                try {
                    this.attributes = JSON.parseObject(strs[5], ATTRIBUTES_TYPE_REFERENCE.getType());
                } catch (Exception e) {
                    // ignore exception when parse failed, cause map's key/value can have ' ' char.
                }
            }

            return true;
        }

        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean isOrder) {
        this.order = isOrder;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @JSONField(serialize = false, deserialize = false)
    public TopicMessageType getTopicMessageType() {
        if (attributes == null) {
            return TopicMessageType.NORMAL;
        }
        String content = attributes.get(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName());
        if (content == null) {
            return TopicMessageType.NORMAL;
        }
        return TopicMessageType.valueOf(content);
    }

    @JSONField(serialize = false, deserialize = false)
    public void setTopicMessageType(TopicMessageType topicMessageType) {
        attributes.put(TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(), topicMessageType.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicConfig that = (TopicConfig) o;

        if (readQueueNums != that.readQueueNums) {
            return false;
        }
        if (writeQueueNums != that.writeQueueNums) {
            return false;
        }
        if (perm != that.perm) {
            return false;
        }
        if (topicSysFlag != that.topicSysFlag) {
            return false;
        }
        if (order != that.order) {
            return false;
        }
        if (!Objects.equals(topicName, that.topicName)) {
            return false;
        }
        if (topicFilterType != that.topicFilterType) {
            return false;
        }
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
            + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order=" + order
            + ", attributes=" + attributes + "]";
    }
}

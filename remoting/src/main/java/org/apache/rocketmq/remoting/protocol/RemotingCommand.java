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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCallback;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RemotingCommand {
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    // see : org.apache.rocketmq.common.MQVersion
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC
    // 缓存各个 CommandCustomHeader class 申明的所有字段（各种访问权限，static, 实例字段），但不包括父类中的继承字段
    // 后续 encode header 的时候直接从缓存获取
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
        new HashMap<>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    private static final String BOUNDARY_TYPE_CANONICAL_NAME = BoundaryType.class.getCanonicalName();
    // 来自系统变量 rocketmq.remoting.version
    // see : org.apache.rocketmq.common.MQVersion : V5_3_3
    private static volatile int configVersion = -1;
    private static AtomicInteger requestId = new AtomicInteger(0);

    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        // rocketmq.serialize.type
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!StringUtils.isBlank(protocol)) {
            try {
                // 默认 JSON
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }
    // requestCode 或者 responseCode
    private int code;
    private LanguageCode language = LanguageCode.JAVA;
    // see : org.apache.rocketmq.common.MQVersion : V5_3_3
    private int version = 0;
    // 溢出无所谓，这里就只是需要一个 requestId
    private int opaque = requestId.getAndIncrement();
    // 低位为1 表示 response ,0 表示 request
    private int flag = 0;
    // response msg
    private String remark;
    // customHeader 中自定义的 headers 在序列化的时候会先被写入 extFields 中
    // 然后 extFields 跟随 RemotingCommand 一起被序列化

    // 存储 customHeader 类中非静态的字段 fieldName -> fieldValue
    // 另外 request 请求的一些额外字段也会保存在这里， see: DynamicalExtFieldRPCHook ... 相关 RPCHook
    private HashMap<String, String> extFields;
    // 如果继承 FastCodesHeader ，那么 customHeader 也会被序列化 (RocketMq 序列化方式)
    // see : org.apache.rocketmq.remoting.protocol.RocketMQSerializable.rocketMQProtocolEncode(org.apache.rocketmq.remoting.protocol.RemotingCommand, io.netty.buffer.ByteBuf)

    // JSON 序列化方式则只会序列化 extFields
    // requestHeader
    private transient CommandCustomHeader customHeader; // 一般存放各个 requestCode 对应的请求实体信息
    private transient CommandCustomHeader cachedHeader;

    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;
    // requestBody(JSON序列化)
    private transient byte[] body;
    private boolean suspended;
    // 编解码所需用时
    private transient Stopwatch processTimer;
    private transient List<CommandCallback> callbackList;

    protected RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        // 设置 version ， 来自系统变量 rocketmq.remoting.version
        // see : org.apache.rocketmq.common.MQVersion : V5_3_3
        setCmdVersion(cmd);
        return cmd;
    }

    public static RemotingCommand createResponseCommandWithHeader(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.markResponseType();
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    protected static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand buildErrorResponse(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(classHeader);
        response.setCode(code);
        response.setRemark(remark);
        return response;
    }

    public static RemotingCommand buildErrorResponse(int code, String remark) {
        return buildErrorResponse(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.getDeclaredConstructor().newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            } catch (InvocationTargetException e) {
                return null;
            } catch (NoSuchMethodException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) throws RemotingCommandException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) throws RemotingCommandException {
        return decode(Unpooled.wrappedBuffer(byteBuffer));
    }

    public static RemotingCommand decode(final ByteBuf byteBuffer) throws RemotingCommandException {
        // headerLength: serializeType(1字节) + headerSize（3字节）
        // header
        // body
        int length = byteBuffer.readableBytes();
        int oriHeaderLen = byteBuffer.readInt();
        // 取底 24 位，也就是三个字节表示 header length
        int headerLength = getHeaderLength(oriHeaderLen);
        if (headerLength > length - 4) { // headerLength > header + body
            throw new RemotingCommandException("decode error, bad header length: " + headerLength);
        }

        RemotingCommand cmd = headerDecode(byteBuffer, headerLength, getProtocolType(oriHeaderLen));

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        // 取底 24 位，也就是三个字节表示 header length
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(ByteBuf byteBuffer, int len,
        SerializeType type) throws RemotingCommandException {
        switch (type) {
            case JSON:
                byte[] headerData = new byte[len];
                byteBuffer.readBytes(headerData);
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(byteBuffer, len);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    public static SerializeType getProtocolType(int source) {
        // 取高位字节，表示序列化方式
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    public static int markProtocolType(int source, SerializeType type) {
        return (type.getCode() << 24) | (source & 0x00FFFFFF);
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }
    // 从 extFields 中反序列化 CommandCustomHeader
    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(
        Class<T> classHeader) throws RemotingCommandException {
        return decodeCommandCustomHeader(classHeader, false);
    }

    public <T extends CommandCustomHeader> T decodeCommandCustomHeader(
        Class<T> classHeader, boolean isCached) throws RemotingCommandException {
        if (isCached && cachedHeader != null) {
            return classHeader.cast(cachedHeader);
        }
        cachedHeader = decodeCommandCustomHeaderDirectly(classHeader, true);
        if (cachedHeader == null) {
            return null;
        }
        return classHeader.cast(cachedHeader);
    }
    // 从 extFields 中反序列化 CommandCustomHeader
    public <T extends CommandCustomHeader> T decodeCommandCustomHeaderDirectly(Class<T> classHeader,
        boolean useFastEncode) throws RemotingCommandException {
        T objectHeader;
        try {
            // classHeader 为需要解码的 CommandCustomHeader 实体类
            // 创建对应的 CommandCustomHeader 实例
            objectHeader = classHeader.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
        // 通过 extFields 还原出 CommandCustomHeader，因为在序列化的时候首先会获取 CommandCustomHeader 的所有非静态字段 field
        // 然后将 field 的 name, value 全部装进 extFields 中
        // 反序列化就是从 extFields 中提取出 key, value ，分别是 CommandCustomHeader 对应 field name ,value
        if (this.extFields != null) {
            if (objectHeader instanceof FastCodesHeader && useFastEncode) {
                // FastCodesHeader 有专门的 encode , decode 逻辑
                ((FastCodesHeader) objectHeader).decode(this.extFields);
                objectHeader.checkFields();
                return objectHeader;
            }
            // 通用 CommandCustomHeader，首先获取类中所有字段（包含父类继承的）
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            // 通过 fieldName 到 extFields 中查找 value
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            // 就是开发者在代码中书写类时最自然、最标准的名称形式。
                            //  java.lang.String
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else if (type.equals(BOUNDARY_TYPE_CANONICAL_NAME)) {
                                valueParsed = BoundaryType.getType(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }
                            // 设置 CommandCustomHeader 字段
                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    //make it able to test
    // 获取 class 申明的所有字段（各种访问权限，static, 实例字段）
    Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        // 缓存各个 CommandCustomHeader class 申明的所有字段（各种访问权限，static, 实例字段），但不包括父类中的继承字段
        // 后续 encode header 的时候直接从缓存获取
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            Set<Field> fieldList = new HashSet<>();
            for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
                // 获取 class 申明的所有字段（各种访问权限，static, 实例字段），但不包括父类中的继承字段
                Field[] fields = className.getDeclaredFields();
                fieldList.addAll(Arrays.asList(fields));
            }
            field = fieldList.toArray(new Field[0]);
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }
/**
 *
 *
 类型	                                 getName()	            getSimpleName()	                  getCanonicalName()
String.class	                        java.lang.String	        String	                        java.lang.String
 内部类 Outer$Inner.class	            com.example.Outer$Inner	    Inner	                      com.example.Outer.Inner
 匿名类 new Object(){}.getClass()	    com.example.Test$1	        (空字符串)	                        null
 局部内部类（方法内定义）	                com.example.Test$1Local	     Local	                            null
 基本类型 int.class	                        int	                    int	                                int
 数组 String[].class	                    [Ljava.lang.String;	       String[]	                         java.lang.String[]

 getName()：返回 JVM 内部使用的二进制名称（Binary Name），对于内部类会使用 $ 分隔，数组使用 [L...; 形式。始终不为 null。

 getSimpleName()：返回源代码中给出的底层类的简称，即不包含包名的部分。对于匿名类返回空字符串，对于数组会加上 []。

 getCanonicalName()：返回 Java 语言规范中定义的规范名称。对于匿名类、局部内部类，因为没有规范的表示方式，会返回 null。对于数组，会递归地对组件类型调用 getCanonicalName()，然后加上 []。
 * */
    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }
    // 将 customHeader 类中非静态的 fieldName -> fieldValue 转移到 extFields
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            // 获取 class 申明的所有字段（各种访问权限，static, 实例字段），包括父类中的继承字段
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                // 过滤非 static 字段
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public void fastEncodeHeader(ByteBuf out) {
        int bodySize = this.body != null ? this.body.length : 0;
        int beginIndex = out.writerIndex();
        // skip 8 bytes ， 跳过 length（四字节）和 headerLength（四字节 的位置
        out.writeLong(0);
        int headerSize;
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            if (customHeader != null && !(customHeader instanceof FastCodesHeader)) {
                // 将 customHeader 类中非静态的 fieldName -> fieldValue 转移到 extFields
                // 如果是 FastCodesHeader ，那么这里就不会填充 extFields，后续直接会将 customHeader 序列化
                this.makeCustomHeaderToNet();
            }
            // RocketMQ 的序列化方式
            headerSize = RocketMQSerializable.rocketMQProtocolEncode(this, out);
        } else {
            // 将 customHeader 类中非静态的 fieldName -> fieldValue 转移到 extFields
            this.makeCustomHeaderToNet();
            // 将 RemotingCommand 中除了 body ，customHeader 等被 transient 标注的字段之外剩下的字段全部 JSON 序列化成 bytes (header)
            /**
             *
             * Public 字段：类中的公开字段。
             * Getter 方法：如果有 getXxx() 或 isXxx() 方法，且对应的属性存在，该属性会被序列化。
             * 非static、非transient：Field 必须满足不是静态的且没有被 transient 修饰。
             * 遵循JavaBean规范：fastjson通常依赖POJO的getter方法和字段名进行序列化。
             *
             * */
            byte[] header = RemotingSerializable.encode(this);
            headerSize = header.length;
            // 写入 header
            out.writeBytes(header);
        }
        // length : 4(headerLength) + headerSize + bodySize
        // headerLength: serializeType(1字节) + headerSize（3字节）
        out.setInt(beginIndex, 4 + headerSize + bodySize);
        out.setInt(beginIndex + 4, markProtocolType(headerSize, serializeTypeCurrentRPC));
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        ((Buffer) result).flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY; // flag 低2位为1
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) { // flag 最低位为 1
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @JSONField(serialize = false)
    public boolean isSuspended() {
        return suspended;
    }

    @JSONField(serialize = false)
    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>(256);
        }
        extFields.put(key, value);
    }

    public void addExtFieldIfNotExist(String key, String value) {
        extFields.putIfAbsent(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
            + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
            + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }

    public Stopwatch getProcessTimer() {
        return processTimer;
    }

    public void setProcessTimer(Stopwatch processTimer) {
        this.processTimer = processTimer;
    }

    public List<CommandCallback> getCallbackList() {
        return callbackList;
    }

    public void setCallbackList(List<CommandCallback> callbackList) {
        this.callbackList = callbackList;
    }
}

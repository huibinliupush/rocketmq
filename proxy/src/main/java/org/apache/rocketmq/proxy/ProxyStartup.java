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

package org.apache.rocketmq.proxy;

import com.google.common.collect.Lists;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.RemotingProtocolServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProxyStartup {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final ProxyStartAndShutdown PROXY_START_AND_SHUTDOWN = new ProxyStartAndShutdown();

    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }
    // proxy 的本地模式主要开三个监听端口，分别是作为 proxy 角色的 grpc 协议端口和 remote 协议端口
    // 以及作为 broker 的监听端口

    // proxy 配置默认会加载 rocket home 中的 conf 目录中的 rmq-proxy.json 文件内容
    // 如果明确指定 -pc 那么就加载指定的文件内容 (json 格式)
    // broker 配置默认会加载 rocket home 中的 conf 目录中的 broker.conf 文件内容
    // org.apache.rocketmq.proxy.config.ProxyConfig.brokerConfigPath

    // -bc(Broker config file path for local mode) , -pc(Proxy config file path) , -pm(Proxy run in local or cluster mode)
    // -h , -n
    public static void main(String[] args) {
        try {
            // parse argument from command line
            CommandLineArgument commandLineArgument = parseCommandLineArgument(args);
            // 初始化 ConfigurationManager （通过 -pc 中指定的配置以及 CommandLine 生成最终配置）
            initConfiguration(commandLineArgument);

            // init thread pool monitor for proxy. 定时打印所有存活线程的 jstack
            initThreadPoolMonitor();
            // grpc 的 threadpool
            ThreadPoolExecutor executor = createServerExecutor();
            // 区分 cluster 模式和 local 模式
            // local 模式创建 broker 启动
            MessagingProcessor messagingProcessor = createMessagingProcessor(); // 重点步骤

            // create grpcServer 监听 8081
            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor, ConfigurationManager.getProxyConfig().getGrpcServerPort())
                .addService(createServiceProcessor(messagingProcessor))
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .configInterceptor()
                .shutdownTime(ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS)
                .build();
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(grpcServer);
            // proxy 也支持 rocketmq 的 remote 协议，客户端也可以通过 remote 协议来与 proxy 交互
            // With remoting protocol in proxy, the remoting(协议) client is able to connect to proxy in the unified architecture,
            // which is stateless and has separate computing and storage. The proxy will focus on traffic management,
            // connection management, and observability while the broker pays more attention to
            // performance, storage, low latency, and high availability.
            // 监听 8080
            RemotingProtocolServer remotingServer = new RemotingProtocolServer(messagingProcessor);
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(remotingServer);

            // start servers one by one.
            PROXY_START_AND_SHUTDOWN.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("try to shutdown server");
                try {
                    PROXY_START_AND_SHUTDOWN.preShutdown();
                    PROXY_START_AND_SHUTDOWN.shutdown();
                } catch (Exception e) {
                    log.error("err when shutdown rocketmq-proxy", e);
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("find an unexpect err.", e);
            System.exit(1);
        }

        System.out.printf("%s%n", new Date() + " rocketmq-proxy startup successfully");
        log.info(new Date() + " rocketmq-proxy startup successfully");
    }

    protected static void initConfiguration(CommandLineArgument commandLineArgument) throws Exception {
        if (StringUtils.isNotBlank(commandLineArgument.getProxyConfigPath())) {
            System.setProperty(Configuration.CONFIG_PATH_PROPERTY, commandLineArgument.getProxyConfigPath());
        }
        // 初始化 proxyHome 默认为 ROCKETMQ_HOME
        ConfigurationManager.initEnv();
        // 从配置文件 -pc 中加载 proxyConfig , authConfig
        ConfigurationManager.intConfig();
        // command line 优先级最高, 设置 NamesrvAddr ， BrokerConfigPath ， ProxyMode
        setConfigFromCommandLineArgument(commandLineArgument);
        log.info("Current configuration: " + ConfigurationManager.formatProxyConfig());

    }

    protected static CommandLineArgument parseCommandLineArgument(String[] args) {
        // -bc(Broker config file path for local mode) , -pc(Proxy config file path) , -pm(Proxy run in local or cluster mode)
        // -h , -n
        CommandLine commandLine = ServerUtil.parseCmdLine("mqproxy", args,
            buildCommandlineOptions(), new DefaultParser());
        if (commandLine == null) {
            throw new RuntimeException("parse command line argument failed");
        }

        CommandLineArgument commandLineArgument = new CommandLineArgument();
        // 将 CommandLine 中的 longOpt 和 value 设置到 CommandLineArgument 中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), commandLineArgument);
        return commandLineArgument;
    }

    private static Options buildCommandlineOptions() {
        // -h , -n
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        Option opt = new Option("bc", "brokerConfigPath", true, "Broker config file path for local mode");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pc", "proxyConfigPath", true, "Proxy config file path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pm", "proxyMode", true, "Proxy run in local or cluster mode");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static void setConfigFromCommandLineArgument(CommandLineArgument commandLineArgument) {
        if (StringUtils.isNotBlank(commandLineArgument.getNamesrvAddr())) {
            ConfigurationManager.getProxyConfig().setNamesrvAddr(commandLineArgument.getNamesrvAddr());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getBrokerConfigPath())) {
            ConfigurationManager.getProxyConfig().setBrokerConfigPath(commandLineArgument.getBrokerConfigPath());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getProxyMode())) {
            ConfigurationManager.getProxyConfig().setProxyMode(commandLineArgument.getProxyMode());
        }
    }

    protected static MessagingProcessor createMessagingProcessor() {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        MessagingProcessor messagingProcessor;

        if (ProxyMode.isClusterMode(proxyModeStr)) {
            // DefaultMessagingProcessor
            messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            ProxyMetricsManager proxyMetricsManager = ProxyMetricsManager.initClusterMode(ConfigurationManager.getProxyConfig());
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(proxyMetricsManager);
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            // 根据 proxyConfig 中的 -bc , -n 配置创建 BrokerController
            BrokerController brokerController = createBrokerController();
            ProxyMetricsManager.initLocalMode(brokerController.getBrokerMetricsManager(), ConfigurationManager.getProxyConfig());
            StartAndShutdown brokerControllerWrapper = new StartAndShutdown() {
                @Override
                public void start() throws Exception {
                    brokerController.start();
                    String tip = "The broker[" + brokerController.getBrokerConfig().getBrokerName() + ", "
                        + brokerController.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
                    if (null != brokerController.getBrokerConfig().getNamesrvAddr()) {
                        tip += " and name server is " + brokerController.getBrokerConfig().getNamesrvAddr();
                    }
                    log.info(tip);
                }

                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(brokerControllerWrapper);
            messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(messagingProcessor);
        return messagingProcessor;
    }

    private static GrpcMessagingApplication createServiceProcessor(MessagingProcessor messagingProcessor) {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(application);
        return application;
    }

    protected static BrokerController createBrokerController() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        List<String> brokerStartupArgList = Lists.newArrayList("-c", config.getBrokerConfigPath());
        if (StringUtils.isNotBlank(config.getNamesrvAddr())) {
            brokerStartupArgList.add("-n");
            brokerStartupArgList.add(config.getNamesrvAddr());
        }
        String[] brokerStartupArgs = brokerStartupArgList.toArray(new String[0]);
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    public static ThreadPoolExecutor createServerExecutor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        // 16 + PROCESSOR_NUMBER * 2
        int threadPoolNums = config.getGrpcThreadPoolNums();
        // 100000
        int threadPoolQueueCapacity = config.getGrpcThreadPoolQueueCapacity();
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );
        PROXY_START_AND_SHUTDOWN.appendShutdown(executor::shutdown);
        return executor;
    }

    public static void initThreadPoolMonitor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(
            LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
            LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
            config.isEnablePrintJstack(), config.getPrintJstackInMillis(),
            config.getPrintThreadPoolStatusInMillis());
        ThreadPoolMonitor.init();
    }
}

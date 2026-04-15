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
package org.apache.rocketmq.tools.command.topic;

import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateTopicSubCommand implements SubCommand {
    // updateTopic -n localhost:9876 -t testCreateNormalTopic -c DefaultCluster
    @Override
    public String commandName() {
        return "updateTopic";
    }

    @Override
    public String commandDesc() {
        return "Update or create topic.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        // https://chat.deepseek.com/a/chat/s/5468763d-da82-4c03-9c1c-fb5347b25ec3
        /**
         *
         * 互斥性：在同一个 OptionGroup 中，多个选项是互斥的。这意味着在解析命令行时，如果用户尝试指定了该组中的多个选项，Apache Commons CLI 库会抛出 AlreadySelectedException 异常，提示选项冲突。
         *
         * 必需性验证：你可以通过 setRequired(true) 方法将一个 OptionGroup 标记为必需的。这样，解析命令行参数时，如果该组中没有任何一个选项被提供，库会提示缺少必需的选项。
         * */
        OptionGroup optionGroup = new OptionGroup(); // 实现 Option 互斥

        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        optionGroup.addOption(opt);
        // -b -c 只能指定一个，不能两个同时指定
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("a", "attributes", true, "attribute(+a=b,+c=d,-e)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "readQueueNums", true, "set read queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "writeQueueNums", true, "set write queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "order", true, "set topic's order(true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "unit", true, "is unit topic (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "hasUnitSub", true, "has unit sub (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(commandLine.getOptionValue('t').trim());

            if (commandLine.hasOption('a')) {
                String attributesModification = commandLine.getOptionValue('a').trim();
                // +key1=value1,+key2=value2,-key3,+key4=value4
                // key3 的 value 为 空 ""
                Map<String, String> attributes = AttributeParser.parseToMap(attributesModification);
                topicConfig.setAttributes(attributes);
            }

            // readQueueNums
            if (commandLine.hasOption('r')) {
                topicConfig.setReadQueueNums(Integer.parseInt(commandLine.getOptionValue('r').trim()));
            }

            // writeQueueNums
            if (commandLine.hasOption('w')) {
                topicConfig.setWriteQueueNums(Integer.parseInt(commandLine.getOptionValue('w').trim()));
            }

            // perm
            if (commandLine.hasOption('p')) {
                topicConfig.setPerm(Integer.parseInt(commandLine.getOptionValue('p').trim()));
            }

            boolean isUnit = false;
            if (commandLine.hasOption('u')) {
                isUnit = Boolean.parseBoolean(commandLine.getOptionValue('u').trim());
            }

            boolean isCenterSync = false;
            if (commandLine.hasOption('s')) {
                isCenterSync = Boolean.parseBoolean(commandLine.getOptionValue('s').trim());
            }

            int topicCenterSync = TopicSysFlag.buildSysFlag(isUnit, isCenterSync);
            topicConfig.setTopicSysFlag(topicCenterSync);

            boolean isOrder = false;
            if (commandLine.hasOption('o')) {
                isOrder = Boolean.parseBoolean(commandLine.getOptionValue('o').trim());
            }
            topicConfig.setOrder(isOrder);

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);

                if (isOrder) {
                    // 通过 brokerAddr 到 nameServer 中获取 brokerName
                    String brokerName = CommandUtil.fetchBrokerNameByAddr(defaultMQAdminExt, addr);
                    // brokerName:topicWriteQueueNums
                    String orderConf = brokerName + ":" + topicConfig.getWriteQueueNums();
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(), orderConf, false);
                    System.out.printf("%s%n", String.format("set broker orderConf. isOrder=%s, orderConf=[%s]",
                        isOrder, orderConf.toString()));
                }
                System.out.printf("create topic to %s success.%n", addr);
                System.out.printf("%s%n", topicConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                // 向集群中所有 master broker 创建 topic
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                // 获取集群中所有副本集的 master 地址
                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    // 向所有 master broker 添加 topic
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    System.out.printf("create topic to %s success.%n", addr);
                }

                if (isOrder) {
                    Set<String> brokerNameSet =
                        CommandUtil.fetchBrokerNameByClusterName(defaultMQAdminExt, clusterName);
                    // brokerName1:topicWriteQueueNums;brokerName2:topicWriteQueueNums;brokerName3:topicWriteQueueNums
                    StringBuilder orderConf = new StringBuilder();
                    String splitor = "";
                    for (String s : brokerNameSet) {
                        orderConf.append(splitor).append(s).append(":")
                            .append(topicConfig.getWriteQueueNums());
                        splitor = ";";
                    }
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(),
                        orderConf.toString(), true);
                    System.out.printf("set cluster orderConf. isOrder=%s, orderConf=[%s]%n", isOrder, orderConf);
                }

                System.out.printf("%s%n", topicConfig);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}

/**
 * Copyright (c) KMG. All Rights Reserved..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.sbk.RocketMQ;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import io.sbk.api.AsyncReader;
import io.sbk.api.Storage;
import io.sbk.api.Parameters;
import io.sbk.api.Writer;
import io.sbk.api.Reader;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

/**
 * Class for RocketMQ driver.
 */
public class RocketMQ implements Storage<byte[]> {
    private final static String CONFIGFILE = "RocketMQ.properties";
    private static final String DEFAULT_CLUSTER = "DefaultCluster";
    private static final Random RANDOM = new Random();

    private String clusterName;
    private String topicName;
    private String namesAdr;
    private String subscriptionName;
    private int partitions;
    private DefaultMQAdminExt rmqAdmin;
    private RocketMQClientConfig rmqClientConfig;

    public static String getRandomString() {
        byte[] buffer = new byte[5];
        RANDOM.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }

    @Override
    public void addArgs(final Parameters params) {
        params.addOption("cluster", true, "Cluster name default: "+DEFAULT_CLUSTER);
        params.addOption("topic", true, "Topic name");
        params.addOption("nameserver", true, "Name Server URI");
        params.addOption("partitions", true, "Number of partitions of the topic (default: 1)");
    }

    @Override
    public void parseArgs(final Parameters params) throws IllegalArgumentException {
        topicName =  params.getOptionValue("topic", null);
        namesAdr = params.getOptionValue("nameserver", null);
        if (namesAdr == null) {
            throw new IllegalArgumentException("Error: Must specify Name server IP address");
        }

        if (topicName == null) {
            throw new IllegalArgumentException("Error: Must specify Topic Name");
        }
        subscriptionName = topicName + getRandomString();
        clusterName =  params.getOptionValue("cluster", DEFAULT_CLUSTER);
        partitions = Integer.parseInt(params.getOptionValue("partitions", "1"));
        final ObjectMapper mapper = new ObjectMapper(new JavaPropsFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            rmqClientConfig = mapper.readValue(getClass().getClassLoader().getResourceAsStream(CONFIGFILE),
                    RocketMQClientConfig.class);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IllegalArgumentException(ex);
        }
     }

    @Override
    public void openStorage(final Parameters params) throws  IOException {
        this.rmqAdmin = new DefaultMQAdminExt();
        this.rmqAdmin.setNamesrvAddr(namesAdr);
        this.rmqAdmin.setInstanceName("AdminInstance-" + getRandomString());
        try {
            this.rmqAdmin.start();
        } catch (MQClientException ex) {
            ex.printStackTrace();
            throw new IOException("MQClientException occurred");
        }
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setOrder(false);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(partitions);
        topicConfig.setWriteQueueNums(partitions);
        topicConfig.setTopicName(topicName);

        try {
            Set<String> brokerList = CommandUtil.fetchMasterAddrByClusterName(this.rmqAdmin, clusterName);
            topicConfig.setReadQueueNums(Math.max(1, partitions / brokerList.size()));
            topicConfig.setWriteQueueNums(Math.max(1, partitions / brokerList.size()));

            for (String brokerAddr: brokerList) {
                this.rmqAdmin.createAndUpdateTopicConfig(brokerAddr, topicConfig);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException(String.format("Failed to create topic [%s] to cluster [%s]", topicName, clusterName), ex);
        }

    }

    @Override
    public void closeStorage(final Parameters params) throws IOException {
        this.rmqAdmin.shutdown();
    }

    @Override
    public Writer createWriter(final int id, final Parameters params) {
        try {
            return new RocketMQWriter(id, params, namesAdr, topicName, rmqClientConfig);
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public Reader createReader(final int id, final Parameters params) {
        return null;
    }

    @Override
    public AsyncReader createAsyncReader(final int id, final Parameters params) {
        try {
            return new RocketMQAsyncReader(id, params, namesAdr, topicName, rmqClientConfig, subscriptionName);
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

}

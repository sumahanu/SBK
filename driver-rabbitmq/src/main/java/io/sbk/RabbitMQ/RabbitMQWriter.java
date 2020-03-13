/**
 * Copyright (c) KMG. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.sbk.RabbitMQ;
import io.sbk.api.QuadConsumer;
import io.sbk.api.Writer;
import io.sbk.api.Parameters;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Class for RabbitMQ Writer.
 */
public class RabbitMQWriter implements Writer<byte[]> {
    final private static BasicProperties DEFAULT_PROPS = new BasicProperties();
    final private String key;
    final private Channel channel;
    final private String topicName;
    final private  ConfirmListener listener;
    final private ConcurrentHashMap<Long, Long> startTimeConcurrentHashMap;
    final private boolean isPersist;
    final private int size;
    private  volatile SortedSet<Long> ackSet;
    private QuadConsumer record;

    public RabbitMQWriter(int writerID, Parameters params,
                          Connection connection, String topicName, boolean isPersist ) throws IOException {
        this.key = String.valueOf(writerID);
        this.isPersist = isPersist;
        this.topicName = topicName;
        this.size = params.getRecordSize();
        channel = connection.createChannel();
        channel.exchangeDeclare(topicName, BuiltinExchangeType.FANOUT);
        channel.confirmSelect();
        ackSet = Collections.synchronizedSortedSet(new TreeSet<Long>());
        startTimeConcurrentHashMap = new ConcurrentHashMap<>();
        this.listener = new ConfirmListener() {

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    SortedSet<Long> treeHeadSet = ackSet.headSet(deliveryTag + 1);
                    synchronized (ackSet) {
                        for (Iterator iterator = treeHeadSet.iterator(); iterator.hasNext();) {
                            long value = (long) iterator.next();
                            iterator.remove();
                            Long startTime = startTimeConcurrentHashMap.get(value);
                            if (startTime != null && record != null) {
                                    record.accept(startTime, System.currentTimeMillis(), size, 1);
                            }
                            startTimeConcurrentHashMap.remove(value);
                        }
                        treeHeadSet.clear();
                    }

                } else {
                    Long startTime  = startTimeConcurrentHashMap.get(deliveryTag);
                    if (startTime != null && record != null) {
                        record.accept(startTime, System.currentTimeMillis(), size, 1);
                    }
                    startTimeConcurrentHashMap.remove(deliveryTag);
                    ackSet.remove(deliveryTag);
                }
            }

            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    SortedSet<Long> treeHeadSet = ackSet.headSet(deliveryTag + 1);
                    synchronized (ackSet) {
                        for (long value : treeHeadSet) {
                            Long startTime = startTimeConcurrentHashMap.get(value);
                            if (startTime != null && record != null) {
                                record.accept(startTime, System.currentTimeMillis(), size, 1);
                            }
                            startTimeConcurrentHashMap.remove(value);
                        }
                        treeHeadSet.clear();
                    }
                } else {
                    Long startTime  = startTimeConcurrentHashMap.get(deliveryTag);
                    if (startTime != null && record != null) {
                        record.accept(startTime, System.currentTimeMillis(), size, 1);
                    }
                    startTimeConcurrentHashMap.remove(deliveryTag);
                    ackSet.remove(deliveryTag);
                }

            }
        };

        channel.addConfirmListener(listener);
        record = null;
    }

    @Override
    public long recordWrite(byte[] data, int size, QuadConsumer record) {
        final long time = System.currentTimeMillis();
        if (this.record == null) {
            this.record = record;
        }
        long msgId = channel.getNextPublishSeqNo();
        ackSet.add(msgId);
        startTimeConcurrentHashMap.putIfAbsent(msgId, time);
        try {
            writeAsync(data);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return time;
    }


    @Override
    public CompletableFuture writeAsync(byte[] data) throws IOException {
        BasicProperties.Builder builder = DEFAULT_PROPS.builder().timestamp(new Date());
        if (isPersist) {
            builder.deliveryMode(2);
        }
        BasicProperties props = builder.build();
        channel.basicPublish(topicName, key, props, data);
        return null;
    }


    @Override
    public void flush() throws IOException {
        return;
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel.isOpen()) {
                channel.removeConfirmListener(listener);
                channel.close();
            }
        } catch (TimeoutException ex) {
            ex.printStackTrace();
            throw new IOException(ex);
        }
    }
}
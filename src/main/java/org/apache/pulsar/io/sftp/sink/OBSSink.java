/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.sftp.sink;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.sftp.common.TaskProgress;
import org.apache.pulsar.io.sftp.common.TaskState;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OBSSink extends AbstractSink<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(OBSSink.class);

    private OBSSinkConfig sinkConfig;
    private Producer<TaskProgress> producer;
    private ExecutorService executor;
    private BlockingQueue<Record<byte[]>> records;
    private String sinkName;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        records = new LinkedBlockingQueue<>();
        OBSSinkConfig sinkConfig = OBSSinkConfig.load(config);
        sinkConfig.validate();
        sinkName = sinkContext.getSinkName();
        producer = sinkContext.getPulsarClient().newProducer(Schema.JSON(TaskProgress.class))
                .topic(sinkConfig.getTaskProgressTopic())
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        this.sinkConfig = sinkConfig;
        // One extra for the File listing thread, and another for the cleanup thread
        executor = Executors.newFixedThreadPool(sinkConfig.getNumWorkers());

        for (int idx = 0; idx < sinkConfig.getNumWorkers(); idx++) {
            executor.execute(new OBSWriteThread(this));
        }
    }

    @Override
    public void write(Record<byte[]> record) {
        try {
            records.put(record);
        } catch (InterruptedException e) {
            record.fail();
            log.error("error", e);
        }
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    public OBSSinkConfig getOBSSinkConfig() {
        return this.sinkConfig;
    }

    public BlockingQueue<Record<byte[]>> getQueue() {
        return records;
    }

    public void sentTaskProgress(Record<byte[]> record, TaskState taskState) {
        String currentDirectory = record.getProperties().get(Constants.FILE_PATH);
        String realAbsolutePath = record.getProperties().get(Constants.FILE_ABSOLUTE_PATH);
        String fileName = record.getProperties().get(Constants.FILE_NAME);
        String modifiedTime = record.getProperties().get(Constants.FILE_MODIFIED_TIME);
        TaskProgress taskProgress = new TaskProgress(currentDirectory + fileName, Constants.TASK_PROGRESS_SFTP,
                Constants.TASK_PROGRESS_SINK_TYPE, sinkName);
        taskProgress.setTimestamp((int) (System.currentTimeMillis() / 1000));
        taskProgress.setProperty("currentDirectory", currentDirectory);
        taskProgress.setProperty("realAbsolutePath", realAbsolutePath);
        taskProgress.setProperty("fileName", fileName);
        taskProgress.setProperty("modifiedTime", modifiedTime);
        taskProgress.setState(taskState);
        this.producer.sendAsync(taskProgress);
    }

}

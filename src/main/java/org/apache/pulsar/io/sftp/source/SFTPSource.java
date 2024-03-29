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
package org.apache.pulsar.io.sftp.source;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.sftp.common.TaskExecutors;
import org.apache.pulsar.io.sftp.common.TaskProgress;
import org.apache.pulsar.io.sftp.common.TaskState;
import org.apache.pulsar.io.sftp.utils.Constants;

/**
 * A simple connector to consume messages from the sftp server.
 * It can be configured to consume files recursively from a given
 * directory.
 */
@NoArgsConstructor
@Slf4j
public class SFTPSource extends PushSource<byte[]> {

    private final BlockingQueue<SFTPFileInfo> workQueue = new LinkedBlockingQueue<>(1000);
    private final BlockingQueue<SFTPFileInfo> inProcess = new LinkedBlockingQueue<>();
    private final BlockingQueue<SFTPFileInfo> recentlyProcessed = new LinkedBlockingQueue<>();
    private TaskExecutors executor;
    private SFTPSourceConfig sftpConfig = null;
    private PulsarClient pulsarClient = null;
    private Consumer<SFTPFileInfo> consumer = null;
    private Producer<TaskProgress> producer = null;
    private String sourceName;
    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sourceName = sourceContext.getSourceName();
        sftpConfig.validate();
        String sftpTaskTopic = sftpConfig.getSftpTaskTopic();
        String taskTopicSubscription = sftpConfig.getSftpTaskTopicSubscriptionName();
        if (StringUtils.isBlank(sftpTaskTopic)) {
            sftpTaskTopic = sourceContext.getOutputTopic() + "-" + "task";
        }
        if (StringUtils.isBlank(taskTopicSubscription)) {
            taskTopicSubscription = sftpTaskTopic + "-" + "sub";
        }
        if (TopicName.get(sftpTaskTopic)
                .equals(TopicName.get(sourceContext.getOutputTopic()))) {
            throw new RuntimeException("sftpTaskTopic can not same with destination-topic-name");
        }

        sftpConfig.setSftpTaskTopic(sftpTaskTopic);
        this.sftpConfig = sftpConfig;
        pulsarClient = sourceContext.getPulsarClient();
        consumer = pulsarClient.newConsumer(Schema.JSON(SFTPFileInfo.class))
                .subscriptionType(SubscriptionType.Shared)
                .topic(sftpConfig.getSftpTaskTopic())
                .ackTimeout(30, TimeUnit.SECONDS)
                .subscriptionName(taskTopicSubscription)
                .subscribe();
        producer = pulsarClient.newProducer(Schema.JSON(TaskProgress.class))
                .topic(sftpConfig.getTaskProgressTopic())
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        // One extra for the File listing thread, and another for the cleanup thread
        executor = new TaskExecutors(sftpConfig.getNumWorkers()
                + sftpConfig.getNumWorkers() / 2 + 2);
        executor.execute(new SFTPListingThread(this));

        for (int idx = 0; idx < sftpConfig.getNumWorkers() / 2 + 1; idx++) {
            executor.execute(new SFTPProcessedThread(this));
        }

        for (int idx = 0; idx < sftpConfig.getNumWorkers(); idx++) {
            executor.execute(new SFTPConsumerThread(this));
        }
    }


    public void sentTaskProgress(SFTPFileInfo fileInfo, TaskState taskState) {
        String currentDirectory = fileInfo.getDirectory();
        String realAbsolutePath = fileInfo.getRealAbsolutePath();
        String fileName = fileInfo.getFileName();
        String modifiedTime = String.valueOf(fileInfo.getModifiedTime());
        TaskProgress taskProgress = new TaskProgress(currentDirectory + fileName, Constants.TASK_PROGRESS_SFTP,
                Constants.TASK_PROGRESS_SOURCE_TYPE, sourceName);
        taskProgress.setTimestamp((int) (System.currentTimeMillis() / 1000));
        taskProgress.setProperty("currentDirectory", currentDirectory);
        taskProgress.setProperty("realAbsolutePath", realAbsolutePath);
        taskProgress.setProperty("fileName", fileName);
        taskProgress.setProperty("modifiedTime", modifiedTime);
        taskProgress.setState(taskState);
        this.producer.sendAsync(taskProgress);
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (consumer != null) {
                consumer.close();
                pulsarClient.close();
            }
        } catch (PulsarClientException e) {
            log.error("PulsarClientException error", e);
        }

    }

    public BlockingQueue<SFTPFileInfo> getInProcess() {
        return this.inProcess;
    }

    public BlockingQueue<SFTPFileInfo> getRecentlyProcessed() {
        return this.recentlyProcessed;
    }

    public PulsarClient getPulsarClient() {
        return this.pulsarClient;
    }

    public Consumer<SFTPFileInfo> getConsumer() {
        return this.consumer;
    }

    public SFTPSourceConfig getSFTPSourceConfig() {
        return this.sftpConfig;
    }

    public Producer getProducer() {
        return this.producer;
    }
}
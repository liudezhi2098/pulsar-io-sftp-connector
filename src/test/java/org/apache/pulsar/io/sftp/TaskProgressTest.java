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
package org.apache.pulsar.io.sftp;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.io.sftp.common.TaskProgress;
import org.apache.pulsar.io.sftp.common.TaskState;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.testng.annotations.Test;

//@Ignore
public class TaskProgressTest {

    @Test
    public void taskProgressTest() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://20.25.99.94:6650")
                .build();
        ConcurrentHashMap<String, ConcurrentHashSet<String>> successSourceTask = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ConcurrentHashSet<String>> failedSourceTask = new ConcurrentHashMap<>();

        ConcurrentHashMap<String, ConcurrentHashSet<String>> successSinkTask = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ConcurrentHashSet<String>> failedSinkTask = new ConcurrentHashMap<>();

        Consumer<TaskProgress> consumer = client.newConsumer(Schema.JSON(TaskProgress.class))
                .subscriptionType(SubscriptionType.Shared)
                .messageListener(new MessageListener<TaskProgress>() {
                    @Override
                    public void received(Consumer<TaskProgress> consumer, Message<TaskProgress> msg) {
                        TaskProgress taskProgress = msg.getValue();
                        System.out.println(taskProgress);
                        String path = taskProgress.getTaskProperties().get("currentDirectory");
                        if (taskProgress.getTaskType().equals(Constants.TASK_PROGRESS_SOURCE_TYPE)) {
                            if (taskProgress.getState().equals(TaskState.Success)) {
                                if (!successSourceTask.containsKey(path)) {
                                    successSourceTask.put(path, new ConcurrentHashSet<String>());
                                }
                                successSourceTask.get(path).add(taskProgress.getTaskId());
                                if (failedSourceTask.containsKey(path)) {
                                    failedSourceTask.get(path).remove(taskProgress.getTaskId());
                                }
                            } else if (taskProgress.getState().equals(TaskState.Failed)) {
                                if (!failedSourceTask.containsKey(path)) {
                                    failedSourceTask.put(path, new ConcurrentHashSet<String>());
                                }
                                failedSourceTask.get(path).add(taskProgress.getTaskId());
                            }
                        } else if (taskProgress.getTaskType().equals(Constants.TASK_PROGRESS_SINK_TYPE)) {
                            if (taskProgress.getState().equals(TaskState.Success)) {
                                if (!successSinkTask.containsKey(path)) {
                                    successSinkTask.put(path, new ConcurrentHashSet<String>());
                                }
                                successSinkTask.get(path).add(taskProgress.getTaskId());
                                if (failedSinkTask.containsKey(path)) {
                                    failedSinkTask.get(path).remove(taskProgress.getTaskId());
                                }
                            } else if (taskProgress.getState().equals(TaskState.Failed)) {
                                if (!failedSinkTask.containsKey(path)) {
                                    failedSinkTask.put(path, new ConcurrentHashSet<String>());
                                }
                                failedSinkTask.get(path).add(taskProgress.getTaskId());
                            }
                        }
                        try {
                            consumer.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .topic("sftp-task-progress")
                .subscriptionName("sftpProgress-sub")
                .subscribe();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    System.out.println("Source ");
                    successSourceTask.entrySet().stream().forEach(entry -> {
                        System.out.println("directory:" + entry.getKey() + ", success:" + entry.getValue().size() + ","
                                + " failed:" + (failedSourceTask.get(entry.getKey()) != null
                                ? failedSourceTask.get(entry.getKey()).size() : 0));
                    });
                    System.out.println("Sink ");
                    successSinkTask.entrySet().stream().forEach(entry -> {
                        System.out.println("directory:" + entry.getKey() + ", success:" + entry.getValue().size() + ","
                                + " failed:" + (failedSinkTask.get(entry.getKey()) != null
                                ? failedSinkTask.get(entry.getKey()).size() : 0));
                    });
                    System.out.println("----------------" + new Date() + "----------------");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        try {
            Thread.sleep(10000000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

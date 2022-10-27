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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.NoArgsConstructor;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A simple connector to consume messages from the sftp server.
 * It can be configured to consume files recursively from a given
 * directory.
 */
@NoArgsConstructor
public class SFTPSource extends PushSource<byte[]> {

    private ExecutorService executor;
    private final BlockingQueue<SFTPSourceRecord> workQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<SFTPSourceRecord> inProcess = new LinkedBlockingQueue<>();
    private final BlockingQueue<SFTPSourceRecord> recentlyProcessed = new LinkedBlockingQueue<>();

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sftpConfig.validate();

        // One extra for the File listing thread, and another for the cleanup thread
        executor = Executors.newFixedThreadPool(sftpConfig.getNumWorkers() + 2);
        executor.execute(new SFTPListingThread(sftpConfig, workQueue, inProcess));
        executor.execute(new ProcessedSFTPThread(sftpConfig, recentlyProcessed));

        for (int idx = 0; idx < sftpConfig.getNumWorkers(); idx++) {
            executor.execute(new SFTPConsumerThread(this, workQueue, inProcess, recentlyProcessed));
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
}
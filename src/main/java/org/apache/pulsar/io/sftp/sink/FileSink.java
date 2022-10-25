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
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSink extends AbstractSink<byte[]> {

    private FileSinkConfig fileSinkConfig;
    private ExecutorService executor;
    private BlockingQueue<Record<byte[]>> records;
    private static final Logger log = LoggerFactory.getLogger(FileSink.class);

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        records = new LinkedBlockingQueue<>();
        FileSinkConfig fileSinkConfig = FileSinkConfig.load(config);
        fileSinkConfig.validate();
        this.fileSinkConfig = fileSinkConfig;
        // One extra for the File listing thread, and another for the cleanup thread
        executor = Executors.newFixedThreadPool(fileSinkConfig.getNumWorkers());

        for (int idx = 0; idx < fileSinkConfig.getNumWorkers(); idx++) {
            executor.execute(new FileWriteThread(this));
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

    public FileSinkConfig getFileSinkConfig() {
        return this.fileSinkConfig;
    }

    public BlockingQueue<Record<byte[]>> getQueue() {
        return records;
    }

}

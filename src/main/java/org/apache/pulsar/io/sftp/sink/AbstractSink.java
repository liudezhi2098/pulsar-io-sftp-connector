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
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Simple abstract sink class for pulsar to file.
 */

public abstract class AbstractSink<T> implements Sink<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSink.class);

    protected BlockingQueue<Record<T>> records;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        records = new LinkedBlockingQueue<>();
        FileSinkConfig fileSinkConfig = FileSinkConfig.load(config);
        fileSinkConfig.validate();
    }

    public abstract void write(Record<T> record);

    @Override
    public void close() {

    }
}

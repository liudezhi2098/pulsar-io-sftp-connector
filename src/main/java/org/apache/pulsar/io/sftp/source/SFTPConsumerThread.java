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

import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.PushSource;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
@Slf4j
public class SFTPConsumerThread extends Thread {

    private final PushSource<byte[]> source;
    private final BlockingQueue<SFTPSourceRecord> workQueue;
    private final BlockingQueue<SFTPSourceRecord> inProcess;
    private final BlockingQueue<SFTPSourceRecord> recentlyProcessed;

    public SFTPConsumerThread(PushSource<byte[]> source,
                              BlockingQueue<SFTPSourceRecord> workQueue,
                              BlockingQueue<SFTPSourceRecord> inProcess,
                              BlockingQueue<SFTPSourceRecord> recentlyProcessed) {
        this.source = source;
        this.workQueue = workQueue;
        this.inProcess = inProcess;
        this.recentlyProcessed = recentlyProcessed;
    }

    public void run() {
        try {
            while (true) {
                SFTPSourceRecord file = workQueue.take();
                boolean added = false;
                do {
                    added = inProcess.add(file);
                } while (!added);
                consumeFile(file);
            }
        } catch (InterruptedException e) {
            // just terminate
            log.error("Take record from  workQueue be interrupted in SFTPConsumerThread",e);
        }
    }

    private void consumeFile(SFTPSourceRecord file) {
            source.consume(file);
            boolean removed = false;
            do {
                removed = inProcess.remove(file);
            } while (!removed);

            boolean added = false;
            do {
                added = recentlyProcessed.add(file);
            } while (!added);
    }


}

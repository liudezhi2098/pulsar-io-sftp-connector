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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.io.core.PushSource;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
public class FileConsumerThread extends Thread {

    private final PushSource<byte[]> source;
    private final BlockingQueue<File> workQueue;
    private final BlockingQueue<File> inProcess;
    private final BlockingQueue<File> recentlyProcessed;

    public FileConsumerThread(PushSource<byte[]> source,
            BlockingQueue<File> workQueue,
            BlockingQueue<File> inProcess,
            BlockingQueue<File> recentlyProcessed) {
        this.source = source;
        this.workQueue = workQueue;
        this.inProcess = inProcess;
        this.recentlyProcessed = recentlyProcessed;
    }

    public void run() {
        try {
            while (true) {
                File file = workQueue.take();

                boolean added = false;
                do {
                    added = inProcess.add(file);
                } while (!added);

                consumeFile(file);
            }
        } catch (InterruptedException ie) {
            // just terminate
        }
    }

    private void consumeFile(File file) {
        final AtomicInteger idx = new AtomicInteger(1);
        try {
            process(file, idx.getAndIncrement(), getContents(file));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
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

    private byte[] getContents(File file) throws IOException {
        DataInputStream reader = new DataInputStream(new FileInputStream(file));
        int nBytesToRead = reader.available();
        byte[] contents = new byte[nBytesToRead];
        reader.read(contents);
        return contents;
    }

    private void process(File srcFile, int lineNumber, byte[] contents) throws NoSuchAlgorithmException, IOException {
        source.consume(new FileSourceRecord(srcFile, lineNumber, contents));
    }

}

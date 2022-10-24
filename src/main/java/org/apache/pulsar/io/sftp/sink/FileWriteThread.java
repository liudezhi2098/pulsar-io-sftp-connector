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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.sftp.source.FileSourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
public class FileWriteThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(FileWriteThread.class);
    private final FileSink sink;

    public FileWriteThread(FileSink sink) {
        this.sink = sink;
    }

    public void run() {
        try {
            while (true) {
                Record<byte[]> record = sink.getQueue().take();
                try {
                    writeToFile(record);
                    record.ack();
                } catch (Exception e) {
                    record.fail();
                    log.error("error", e);
                }
            }
        } catch (InterruptedException ie) {
            // just terminate
        }
    }

    private void writeToFile(Record<byte[]> record) throws IOException {
        RandomAccessFile randomFile = null;
        try {
            Message<byte[]> msg = record.getMessage().get();
            byte[] contents = msg.getValue();
            String name = new File(msg.getProperty(FileSourceRecord.FILE_NAME)).getName();
            String fileName = sink.getFileSinkConfig().getOutDirectory() + "/" + name;
            String md5 = msg.getProperty(FileSourceRecord.FILE_MD5);
            File writeFile = new File(fileName);
            if (writeFile.exists()) {
                writeFile.delete();
            }
            randomFile = new RandomAccessFile(fileName, "rw");
            randomFile.seek(randomFile.length());
            randomFile.write(contents);
            randomFile.close();
            randomFile = null;
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

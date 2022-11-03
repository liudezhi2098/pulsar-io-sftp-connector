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

import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
@Slf4j
public class FileWriteThread extends Thread {

    private final FileSink sink;

    public FileWriteThread(FileSink sink) {
        this.sink = sink;
    }

    public void run() {
        FileSinkConfig sinkConf = sink.getFileSinkConfig();
        try {
            Class clazz = Class.forName(sinkConf.getFileWriteClass());
            Object obj = clazz.newInstance();
            Method method = clazz.getDeclaredMethod("writeToStorage", Record.class, FileSinkConfig.class);
            while (true) {
                Record<byte[]> record = sink.getQueue().take();
                try {
                    method.invoke(obj, record, sinkConf);
                    record.ack();
                } catch (Exception e) {
                    record.fail();
                    log.error("FileWriteThread run error", e);
                }
            }
        } catch (InterruptedException e) {
            // just terminate
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            //todo
            e.printStackTrace();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}

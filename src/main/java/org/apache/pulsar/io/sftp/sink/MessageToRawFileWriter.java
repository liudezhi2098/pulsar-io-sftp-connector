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
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.sftp.common.TaskState;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.apache.pulsar.io.sftp.utils.FileUtil;

/**
 * Write files to local disk in parquet format.
 */
@Slf4j
public class MessageToRawFileWriter implements MessageFileWriter<byte[]> {

    @Override
    public void writeToStorage(Record<byte[]> record, FileSink fileSink) {
        FileSinkConfig sinkConfig = fileSink.getFileSinkConfig();
        RandomAccessFile randomFile = null;
        try {
            Message<byte[]> msg = record.getMessage().get();
            byte[] contents = msg.getValue();
            String name = new File(msg.getProperty(Constants.FILE_NAME)).getName();
            String sftpPath = msg.getProperty(Constants.FILE_ABSOLUTE_PATH);
            String fileName = sinkConfig.getOutDirectory() + "/" + sftpPath + "/" + name;
            File path = new File(sinkConfig.getOutDirectory() + "/" + sftpPath);
            if (!path.exists()) {
                path.mkdirs();
            }
            String originalMD5 = msg.getProperty(Constants.FILE_MD5);
            File writeFile = new File(fileName);
            if (writeFile.exists()) {
                writeFile.delete();
            }
            randomFile = new RandomAccessFile(fileName, "rw");
            try {
                randomFile.seek(randomFile.length());
            } catch (IOException e) {
                e.printStackTrace();
            }
            randomFile.write(contents);
            randomFile.close();
            randomFile = null;
            String currentMD5 = FileUtil.getFileMD5(contents);
            if (!Objects.equals(originalMD5, currentMD5)) {
                fileSink.sentTaskProgress(record, TaskState.Failed);
                throw new IllegalStateException("The md5 value of the current file : " + name
                        + "  is inconsistent with the original file. Current md5 : " + currentMD5 + ". Original md5 : "
                        + originalMD5 + ".");
            } else {
                fileSink.sentTaskProgress(record, TaskState.Success);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
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

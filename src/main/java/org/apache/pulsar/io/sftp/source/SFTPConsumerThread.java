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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.io.sftp.common.SFTPTaskState;
import org.apache.pulsar.io.sftp.common.TaskThread;
import org.apache.pulsar.io.sftp.exception.SFTPFileNotExistException;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
@Slf4j
public class SFTPConsumerThread extends TaskThread {

    private final SFTPSource sftpSource;
    private final SFTPUtil sftp;
    private boolean stop = false;
    private final Consumer<SFTPFileInfo> consumer;

    public SFTPConsumerThread(SFTPSource sftpSource) {
        this.sftpSource = sftpSource;
        SFTPSourceConfig sftpConfig = sftpSource.getSFTPSourceConfig();
        SFTPUtil sftp = new SFTPUtil(sftpConfig.getUsername(), sftpConfig.getPassword(), sftpConfig.getHost(),
                sftpConfig.getPort());
        sftp.login();
        this.sftp = sftp;
        this.consumer = sftpSource.getConsumer();
    }

    public void run() {
        try {
            while (!stop) {
                Message<SFTPFileInfo> msg = null;
                try {
                    msg = consumer.receive();
                    SFTPFileInfo fileInfo = msg.getValue();
                    boolean added = false;
                    do {
                        fileInfo.setState(SFTPTaskState.AddWorkQueue);
                        added = this.sftpSource.getInProcess().add(fileInfo);
                    } while (!added);
                    consumeFile(fileInfo);
                } catch (PulsarClientException e) {
                    log.error("PulsarClientException error", e);
                }
                if (msg != null) {
                    consumer.acknowledge(msg);
                }
            }
        } catch (PulsarClientException e) {
            log.error("PulsarClientException error", e);
            // just terminate
        }
    }

    private void consumeFile(SFTPFileInfo fileInfo) {
        try {
            filterLargerFile(fileInfo);
            process(fileInfo);
            boolean removed = false;
            do {
                removed = this.sftpSource.getInProcess().remove(fileInfo);
            } while (!removed);

            boolean added = false;
            do {
                if (!fileInfo.getState().equals(SFTPTaskState.Failed)) {
                    fileInfo.setState(SFTPTaskState.Ending);
                }
                added = this.sftpSource.getRecentlyProcessed().add(fileInfo);
            } while (!added);
        } catch (SFTPFileNotExistException e) {
            fileInfo.setState(SFTPTaskState.Failed);
        } catch (IOException | NoSuchAlgorithmException e) {
            fileInfo.setState(SFTPTaskState.Failed);
            log.error("consumeFile[{}/{}] error:", fileInfo.getDirectory(), fileInfo.getFileName(), e);
        }
    }

    private void process(SFTPFileInfo fileInfo)
            throws SFTPFileNotExistException, NoSuchAlgorithmException, IOException {
        String fileName = fileInfo.getFileName();
        String currentDirectory = fileInfo.getDirectory();
        String realAbsolutePath = fileInfo.getRealAbsolutePath();
        String modifiedTime = fileInfo.getModifiedTime();
        byte[] file = sftp.download(currentDirectory, fileName);
        if (file == null) {
            throw new SFTPFileNotExistException(String.format("%s/%s not exist!", currentDirectory, fileName));
        }
        SFTPSourceRecord record = new SFTPSourceRecord(fileName, file, currentDirectory, realAbsolutePath,
                modifiedTime);
        sftpSource.consume(record);
    }

    private void filterLargerFile (SFTPFileInfo fileInfo) throws SFTPFileNotExistException {
        String fileName = fileInfo.getFileName();
        String currentDirectory = fileInfo.getDirectory();
        long fileSize = sftp.getFileSize(currentDirectory, fileName);
        if (fileSize == -1) {
            throw new SFTPFileNotExistException(String.format("%s/%s not exist!", currentDirectory, fileName));
        }
        SFTPSourceConfig sftpSourceConfig = sftpSource.getSFTPSourceConfig();
        long maximumSize = sftpSourceConfig.getMaximumSize();
        if (fileSize > maximumSize) {
            fileInfo.setState(SFTPTaskState.Failed);
            String illegalFilePath =  sftpSourceConfig.getIllegalFileDirectory() + "/" + fileInfo.getRealAbsolutePath();
            log.warn("{} file size[{}] > maximumSize[{}], will be moved to illegal file path '{}'",
                    currentDirectory + "/" + fileName, fileSize, maximumSize, illegalFilePath);
        }
    }

    @Override
    public void close() {
        stop = true;
        sftp.logout();
    }
}

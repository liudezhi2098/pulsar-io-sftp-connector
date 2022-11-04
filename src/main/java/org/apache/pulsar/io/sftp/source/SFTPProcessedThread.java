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

import com.jcraft.jsch.SftpException;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.sftp.common.TaskState;
import org.apache.pulsar.io.sftp.common.TaskThread;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Worker thread that cleans up (delete or move to other path) all the files that have been processed.
 */
@Slf4j
public class SFTPProcessedThread extends TaskThread {

    private final SFTPSource sftpSource;
    private final SFTPUtil sftp;
    private boolean stop = false;

    public SFTPProcessedThread(SFTPSource sftpSource) {
        this.sftpSource = sftpSource;
        SFTPSourceConfig sftpConfig = sftpSource.getSFTPSourceConfig();
        SFTPUtil sftp = new SFTPUtil(sftpConfig.getUsername(), sftpConfig.getPassword(), sftpConfig.getHost(),
                sftpConfig.getPort());
        sftp.login();
        this.sftp = sftp;
    }

    public void run() {
        try {
            while (!stop) {
                SFTPFileInfo record = sftpSource.getRecentlyProcessed().take();
                try {
                    handle(record);
                } catch (SftpException e) {
                    log.error("move or delete file [{}/[]] error", record.getDirectory(), record.getFileName(), e);
                }
                record.setState(TaskState.Success);
            }
        } catch (InterruptedException e) {
            // just terminate
        }
    }

    private void handle(SFTPFileInfo fileInfo) throws SftpException {
        String absolutePath = fileInfo.getRealAbsolutePath();
        String fileName = fileInfo.getFileName();
        SFTPSourceConfig sftpConfig = sftpSource.getSFTPSourceConfig();
        if (sftpConfig.getKeepFile()) {
            String oldFilePath = Objects.equals(".", absolutePath) ? sftpConfig.getInputDirectory() :
                    sftpConfig.getInputDirectory() + "/" + absolutePath;
            String newFilePath = Objects.equals(".", absolutePath) ? sftpConfig.getMovedDirectory() :
                    sftpConfig.getMovedDirectory() + "/" + absolutePath;
            if (fileInfo.getState() == TaskState.Failed) {
                newFilePath =  sftpConfig.getIllegalFileDirectory() + "/" + fileInfo.getRealAbsolutePath();
            }
            //if `movedDirectory` not existed, create it
            if (!sftp.isDirExist(newFilePath)) {
                String[] dirs = ("/" + absolutePath).split("/");
                sftp.createDirIfNotExist(dirs, sftpConfig.getMovedDirectory(), dirs.length, 0);
            }
            sftp.rename(oldFilePath + "/" + fileName, newFilePath + "/" + fileName);
            log.info(String.format("moved file %s from '%s' to '%s'", fileName, oldFilePath + "/"
                            + fileName, newFilePath + "/" + fileName));
        } else {
            String filePath = Objects.equals(".", absolutePath) ? sftpConfig.getInputDirectory() + "/" + fileName :
                    sftpConfig.getInputDirectory() + "/" + absolutePath + "/" + fileName;
            sftp.deleteFile(filePath);
            log.info(String.format("Deleted file %s on '%s'", fileName, filePath));
        }
    }

    @Override
    public void close() {
        stop = true;
        sftp.logout();
    }
}

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

import static org.apache.pulsar.io.sftp.utils.Constants.*;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Worker thread that cleans up (delete or move to other path) all the files that have been processed.
 */
@Slf4j
public class ProcessedSFTPThread extends Thread {

    private final BlockingQueue<SFTPSourceRecord> recentlyProcessed;
    private final SFTPSourceConfig fileConfig;

    public ProcessedSFTPThread(SFTPSourceConfig fileConfig, BlockingQueue<SFTPSourceRecord> recentlyProcessed) {
        this.fileConfig = fileConfig;
        this.recentlyProcessed = recentlyProcessed;
    }

    public void run() {
        try {
            while (true) {
                SFTPUtil sftp = new SFTPUtil(fileConfig.getUsername(), fileConfig.getPassword(), fileConfig.getHost(), 22);
                sftp.login();
                SFTPSourceRecord record = recentlyProcessed.take();
                handle(record,sftp);
                sftp.logout();
            }
        } catch (InterruptedException e) {
            // just terminate
            log.error("Interrupted on take record from recentlyProcessed queue",e);
        }
    }

    private void handle(SFTPSourceRecord record,SFTPUtil sftp) {
        String absolutePath = record.getProperties().get(FILE_ABSOLUTE_PATH);
        String fileName = record.getProperties().get(FILE_NAME);
        if (fileConfig.getKeepFile()) {
            String oldFilePath = Objects.equals(".",absolutePath) ? fileConfig.getInputDirectory() : fileConfig.getInputDirectory() + "/" + absolutePath;
            String newFilePath = Objects.equals(".",absolutePath) ? fileConfig.getMovedDirectory() : fileConfig.getMovedDirectory() + "/" + absolutePath;
            //if `movedDirectory` not existed, create it
            if(!sftp.isDirExist(newFilePath)){
                String[] dirs = ("/" + absolutePath).split("/");
                sftp.createDirIfNotExist(dirs,fileConfig.getMovedDirectory(),dirs.length,0);
            }
            sftp.rename(oldFilePath + "/" + fileName,newFilePath + "/" + fileName);
            log.info(String.format("Copied file %s from '%s' to '%s'",fileName,oldFilePath + "/" + fileName,newFilePath + "/" + fileName));
        } else {
            String filePath = Objects.equals(".",absolutePath) ? fileConfig.getInputDirectory() + "/" + fileName : fileConfig.getInputDirectory() + "/" + absolutePath + "/" + fileName;
            sftp.deleteFile(filePath);
            log.info(String.format("Deleted file %s on '%s'",fileName,filePath));
        }
    }
}

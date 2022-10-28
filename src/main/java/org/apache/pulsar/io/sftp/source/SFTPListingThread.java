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

import com.jcraft.jsch.ChannelSftp;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Worker thread that checks the configured input directory for
 * files that meet the provided filtering criteria, and publishes
 * them to a work queue for processing by the SFTPConsumerThreads.
 */
@Slf4j
public class SFTPListingThread extends Thread {

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final BlockingQueue<SFTPSourceRecord> workQueue;
    private final BlockingQueue<SFTPSourceRecord> inProcess;

    private final SFTPSourceConfig fileConfig;

    public SFTPListingThread(SFTPSourceConfig fileConfig,
                             BlockingQueue<SFTPSourceRecord> workQueue,
                             BlockingQueue<SFTPSourceRecord> inProcess) {
        this.workQueue = workQueue;
        this.inProcess = inProcess;
        this.fileConfig = fileConfig;

    }

    public void run() {
        long pollingInterval = Optional.ofNullable(fileConfig.getPollingInterval()).orElse(10000L);

        while (true) {
            SFTPUtil sftp = new SFTPUtil(fileConfig.getUsername(), fileConfig.getPassword(), fileConfig.getHost(), 22);
            sftp.login();
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingInterval) && listingLock.tryLock()) {
                try {
                    String inputDir = fileConfig.getInputDirectory();
                    String illegalFileDir = fileConfig.getIllegalFileDirectory();
                    boolean recurse = fileConfig.getRecurse();
                    Double maximumSize = fileConfig.getMaximumSize();

                    Set<SFTPSourceRecord> listing = new HashSet<>();
                    try {
                        performListing(inputDir, sftp, listing, recurse);
                    } catch (NoSuchAlgorithmException | IOException e) {
                        throw new IllegalStateException(
                                "Cannot read all files from directory: " + inputDir + " , current listing : "
                                        + listing);
                    }
                    if (!listing.isEmpty()) {
                        // remove any files that have been or are currently being processed.
                        listing.removeAll(inProcess);

                        for (SFTPSourceRecord record : listing) {
                            String absolutePath = record.getProperties().get(Constants.FILE_ABSOLUTE_PATH);
                            String realAbsolutePath = absolutePath.replaceFirst(inputDir, "").replaceFirst("/", "");
                            String fileName = record.getProperties().get(Constants.FILE_NAME);
                            if (!workQueue.contains(record)) {
                                //file size > `maximumSize` (default 1M) , move file to `illegalFileDirectory`
                                if (record.getValue().length * 8 > maximumSize) {
                                    String oldFilePath = inputDir + "/" + realAbsolutePath;
                                    String illegalFilePath = illegalFileDir + "/" + realAbsolutePath;
                                    //`illegalFileDir` not existed, create it first
                                    if (!sftp.isDirExist(illegalFilePath)) {
                                        String[] dirs = ("/" + realAbsolutePath).split("/");
                                        sftp.createDirIfNotExist(dirs, illegalFileDir, dirs.length, 0);
                                    }
                                    log.info("File size  >  maximumSize, will be moved to illegal file path : '"
                                            + illegalFilePath + "'");
                                    sftp.rename(oldFilePath + "/" + fileName, illegalFilePath + "/" + fileName);

                                } else {
                                    record.getProperties().put(Constants.FILE_ABSOLUTE_PATH,
                                            StringUtils.isBlank(realAbsolutePath) ? "." : realAbsolutePath);
                                    workQueue.offer(record);
                                    log.info("Add File to work queue : " + record);
                                }
                            }
                        }
                        queueLastUpdated.set(System.currentTimeMillis());
                    }
                } finally {
                    listingLock.unlock();
                    sftp.logout();
                }
            }
            try {
                sleep(pollingInterval - 1);
            } catch (InterruptedException e) {
                // Just ignore
                log.error("Sleeping be interrupted in SFTPListingThread", e);
            }
        }
    }

    private void performListing(final String directory, SFTPUtil sftp, Set<SFTPSourceRecord> listing,
                                Boolean isRecursive)
            throws NoSuchAlgorithmException, IOException {
        boolean ignoreHiddenFiles = fileConfig.getIgnoreHiddenFiles();
        String fileFilter = fileConfig.getFileFilter();
        final Pattern filePattern = Pattern.compile(Optional.ofNullable(fileFilter)
                .orElse("[^\\.].*"));

        Vector<ChannelSftp.LsEntry> fileAndFolderList = sftp.listFiles(directory);
        for (ChannelSftp.LsEntry item : fileAndFolderList) {
            if (!item.getAttrs().isDir()) {
                String fileName = item.getFilename();
                if ((Optional.of(ignoreHiddenFiles).orElse(true) && fileName.startsWith("."))
                        || !filePattern.matcher(fileName).matches()) {
                    //whether ignore hidden file which start with `.` & Filter files that match the  `fileFilter`
                    log.warn("Ignore hidden file or filter file : " + fileName);
                } else {
                    SFTPUtil downloadSftp =
                            new SFTPUtil(fileConfig.getUsername(), fileConfig.getPassword(), fileConfig.getHost(), 22);
                    downloadSftp.login();
                    byte[] file = downloadSftp.download(directory, item.getFilename());
                    if (file == null) {
                        log.error("May download file '" + fileName + "'  from '" + directory
                                + "' failed , please check and download next file");
                        return;
                    }
                    downloadSftp.logout();
                    SFTPSourceRecord record =
                            new SFTPSourceRecord(fileName, file, directory, item.getAttrs().getAtimeString());
                    listing.add(record);

                }
            } else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
                if (isRecursive) {
                    performListing(directory + "/" + item.getFilename(), sftp, listing, true);
                }
            }
        }
    }

}

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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.io.sftp.common.SFTPTaskState;
import org.apache.pulsar.io.sftp.common.TaskThread;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Worker thread that checks the configured input directory for
 * files that meet the provided filtering criteria, and publishes
 * them to a work queue for processing by the SFTPConsumerThreads.
 */
@Slf4j
public class SFTPListingThread extends TaskThread {

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final SFTPUtil sftp;
    private boolean stop = false;
    private final SFTPSource sftpSource;
    private final SFTPSourceConfig sftpConfig;
    private final BlockingQueue<SFTPFileInfo> inProcess;
    private final BlockingQueue<SFTPFileInfo> recentlyProcessed;
    private Producer<SFTPFileInfo> producer;

    public SFTPListingThread(SFTPSource sftpSource) throws PulsarClientException {
        this.sftpSource = sftpSource;
        this.inProcess = sftpSource.getInProcess();
        this.recentlyProcessed = sftpSource.getRecentlyProcessed();
        this.sftpConfig = sftpSource.getSFTPSourceConfig();
        SFTPUtil sftp = new SFTPUtil(sftpConfig.getUsername(), sftpConfig.getPassword(), sftpConfig.getHost(),
                sftpConfig.getPort());
        sftp.login();
        this.sftp = sftp;
    }

    public void run() {
        long pollingInterval = Optional.ofNullable(sftpConfig.getPollingInterval()).orElse(10000L);
        try {
            producer = sftpSource.getPulsarClient().newProducer(Schema.JSON(SFTPFileInfo.class))
                    .accessMode(ProducerAccessMode.WaitForExclusive)
                    .topic(sftpConfig.getSftpTaskTopic())
                    .create();
        } catch (PulsarClientException e) {
            log.error("create producer error,", e);
        }
        Set<SFTPFileInfo> lastFileslisting = new HashSet<>();
        while (!stop) {
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingInterval) && listingLock.tryLock()) {
                try {
                    String inputDir = sftpConfig.getInputDirectory();
                    String illegalFileDir = sftpConfig.getIllegalFileDirectory();
                    boolean recurse = sftpConfig.getRecurse();

                    Set<SFTPFileInfo> listing = new HashSet<>();
                    try {
                        performListing(inputDir, inputDir, illegalFileDir, listing, recurse);
                    } catch (NoSuchAlgorithmException | IOException e) {
                        throw new IllegalStateException(
                                "Cannot read all files from directory: " + inputDir + " , current listing : "
                                        + listing);
                    }
                    if (!listing.isEmpty()) {
                        // remove any files that have been or are currently being processed.
                        Set<SFTPFileInfo> listingSwap = new HashSet<>();
                        listingSwap.addAll(listing);
                        listing.removeAll(lastFileslisting);
                        if (sftpConfig.getKeepFile()) {
                            listing.removeAll(recentlyProcessed);
                        }
                        for (SFTPFileInfo fileInfo : listing) {
                            String absolutePath = fileInfo.getDirectory();
                            String fileName = fileInfo.getFileName();
                            if (!inProcess.contains(fileInfo)
                                    && !recentlyProcessed.contains(fileInfo)) {
                                fileInfo.setState(SFTPTaskState.AddWorkQueue);
                                try {
                                    producer.send(fileInfo);
                                    queueLastUpdated.set(System.currentTimeMillis());
                                    log.info("Add file[{}] to topic[{}] success ", absolutePath + "/" + fileName,
                                            sftpConfig.getSftpTaskTopic());
                                } catch (PulsarClientException e) {
                                    log.error("Add file[{}] to topic[{}] error ", absolutePath + "/" + fileName,
                                            sftpConfig.getSftpTaskTopic());
                                }
                            }
                        }
                        lastFileslisting = listingSwap;
                    }
                } finally {
                    listingLock.unlock();
                }
            }
            try {
                sleep(pollingInterval - 1);
            } catch (InterruptedException e) {
                // Just ignore
            }
        }
    }

    private void performListing(final String baseDirectory, final String currentDirectory, String illegalFileDir,
                                Set<SFTPFileInfo> listing,
                                Boolean isRecursive)
            throws NoSuchAlgorithmException, IOException {
        boolean ignoreHiddenFiles = sftpConfig.getIgnoreHiddenFiles();
        String fileFilter = sftpConfig.getFileFilter();
        final Pattern filePattern = Pattern.compile(Optional.ofNullable(fileFilter)
                .orElse("[^\\.].*"));
        Vector<ChannelSftp.LsEntry> fileAndFolderList = sftp.listFiles(currentDirectory);
        for (ChannelSftp.LsEntry item : fileAndFolderList) {
            if (!item.getAttrs().isDir()) {
                String fileName = item.getFilename();
                if ((Optional.of(ignoreHiddenFiles).orElse(true) && fileName.startsWith("."))
                        || !filePattern.matcher(fileName).matches()) {
                    //whether ignore hidden file which start with `.` & Filter files that match the  `fileFilter`
                    if (log.isDebugEnabled()) {
                        log.warn("Ignore hidden file or filter file : {} ", currentDirectory + "/" + fileName);
                    }
                } else {
                    String realAbsolutePath = currentDirectory.replaceFirst(baseDirectory, "")
                            .replaceFirst("/", "");
                    SFTPFileInfo fileInfo = new SFTPFileInfo(fileName, currentDirectory, realAbsolutePath,
                            item.getAttrs().getAtimeString());
                    if (listing.size() <= sftpConfig.getMaxFileNumListing()) {
                        listing.add(fileInfo);
                    } else {
                        return;
                    }
                }
            } else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
                if (isRecursive) {
                    performListing(baseDirectory, currentDirectory + "/" + item.getFilename(),
                            illegalFileDir, listing, true);
                }
            }
        }
    }

    @Override
    public void close() {
        stop = true;
        sftp.logout();
        if (producer != null) {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                log.error("close producer error,", e);
            }
        }
    }
}

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
package org.apache.pulsar.io.sftp.utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

@NoArgsConstructor
@Slf4j
public class SFTPUtil {

    private ChannelSftp sftp;
    private Session session;
    private String username;
    private String password;
    private String privateKey;
    private String host;
    private int port = 22;

    public SFTPUtil(String username, String password, String host, int port) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    public SFTPUtil(String username, String host, int port, String privateKey) {
        this.username = username;
        this.host = host;
        this.port = port;
        this.privateKey = privateKey;
    }

    private void checkState() {
        if (session == null || sftp == null || !session.isConnected() || !sftp.isConnected() || sftp.isClosed()) {
            try {
                logout();
                login();
            } catch (Exception e) {
                log.error("check sftp server state error, ", e);
            }
        }

    }
    /**
     * login server.
     */
    public void login() {
        try {
            JSch jsch = new JSch();
            if (privateKey != null) {
                jsch.addIdentity(privateKey);
            }
            session = jsch.getSession(username, host, port);
            if (password != null) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftp = (ChannelSftp) channel;

        } catch (JSchException e) {
            log.error("Failed to login the sftp server ", e);
        }
    }

    /**
     * close server.
     */
    public void logout() {
        if (sftp != null) {
            if (sftp.isConnected()) {
                sftp.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }

    /**
     * List all files in sourcePath.
     *
     * @param directory
     * @return
     * @throws SftpException
     */
    public Vector<ChannelSftp.LsEntry> listFiles(String directory) {
        checkState();
        try {
            return sftp.ls(directory);
        } catch (SftpException e) {
            log.error("List files in directory : {} failed", directory, e);
        }
        return new Vector<>();
    }

    /**
     * rename a file , can be used for remove operation.
     *
     * @param oldFilePath
     * @param newFilePath
     * @throws Exception
     */
    public void rename(String oldFilePath, String newFilePath) throws SftpException {
        checkState();
        sftp.rename(oldFilePath, newFilePath);
    }

    /**
     * upload file.
     *
     * @param directory
     * @param uploadFile
     */
    public void upload(String directory, String uploadFile) {
        checkState();
        try {
            File file = new File(uploadFile);
            sftp.put(Files.newInputStream(file.toPath()), directory + "/" + file.getName());
            log.info("File: '{}' is upload to '{}' success", uploadFile, directory);
        } catch (SftpException | IOException e) {
            log.error("Upload file '{}' to  remote directory '{}' failed", uploadFile, directory, e);
        }

    }

    /**
     * get file size.
     *
     * @param directory
     * @param downloadFile
     * @return
     */
    public long getFileSize(String directory, String downloadFile) {
        checkState();
        if (directory != null && !"".equals(directory)) {
            try {
                SftpATTRS sftpATTRS = sftp.lstat(directory + "/" + downloadFile);
                long fileSize = sftpATTRS.getSize();
                return fileSize;
            } catch (SftpException e) {
                if (log.isDebugEnabled()) {
                    log.error("get file  size from remote directory {} failed", downloadFile, directory, e);
                }
            }
        }
        return -1;
    }

    /**
     * download file.
     *
     * @param directory
     * @param downloadFile
     * @return
     */
    public byte[] download(String directory, String downloadFile) {
        checkState();
        if (directory != null && !"".equals(directory)) {
            try {
                long startTime = System.currentTimeMillis();
                InputStream is = sftp.get(directory + "/" + downloadFile);
                byte[] fileData = IOUtils.toByteArray(is);
                is.close();
                long useTime = System.currentTimeMillis() - startTime;
                log.info("File: '{}/{}' is download success, size({}) use time {} ms", directory, downloadFile,
                        fileData.length, useTime);
                return fileData;
            } catch (SftpException | IOException e) {
                if (e.getMessage().contains("No such file")) {
                    log.warn("file {} not exist.", downloadFile, e);
                } else {
                    log.error("Download file '{}' from  remote directory '{}' failed", downloadFile, directory, e);
                }
            }
        }
        return null;
    }

    /**
     * batch files download.
     *
     * @param directory
     * @param fileList
     */
    public void recursiveDownloadFile(String directory, List<byte[]> fileList, Boolean isRecursive) {
        checkState();
        Vector<ChannelSftp.LsEntry> fileAndFolderList = listFiles(directory);
        for (ChannelSftp.LsEntry item : fileAndFolderList) {
            if (!item.getAttrs().isDir()) {
                fileList.add(download(directory, item.getFilename()));
            } else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
                if (isRecursive) {
                    recursiveDownloadFile(directory + "/" + item.getFilename(), fileList, true);
                }
            }
        }
    }

    /**
     * directory weather exist.
     *
     * @param directory
     * @return
     */
    public boolean isDirExist(String directory) {
        checkState();
        boolean isDirExistFlag = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(directory);
            isDirExistFlag = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().equals("no such file")) {
                isDirExistFlag = false;
            }
        }
        return isDirExistFlag;
    }

    /**
     * create directory.
     *
     * @param directory
     */
    public void createDir(String directory) {
        checkState();
        try {
            sftp.mkdir(directory);
            log.info("Create directory '{}' success", directory);
        } catch (SftpException e) {
            log.error("Create directory '{}' failed", directory, e);
        }
    }

    /**
     * create multi-level directory.
     *
     * @param dirs
     * @param tempPath
     * @param length
     * @param index
     */
    public void createDirIfNotExist(String[] dirs, String tempPath, int length, int index) {
        checkState();
        index++;
        if (index < length) {
            tempPath += "/" + dirs[index];
        } else {
            return;
        }
        try {
            if (isDirExist(tempPath)) {
                sftp.cd(tempPath);
            } else {
                sftp.mkdir(tempPath);
                sftp.cd(tempPath);
            }
            if (index < length) {
                createDirIfNotExist(dirs, tempPath, length, index);
            }
        } catch (SftpException ex) {
            log.error("create directory [{}]", tempPath, ex);
            try {
                sftp.mkdir(tempPath);
                sftp.cd(tempPath);
            } catch (SftpException e) {
                log.error("create directory [{}] failed", tempPath,  e);
            }
            log.info("cd directory [{}]", tempPath);
            createDirIfNotExist(dirs, tempPath, length, index);
        }
    }

    /**
     * remove directory.
     *
     * @param directory
     */
    public void removeDir(String directory) {
        checkState();
        try {
            sftp.rmdir(directory);
            log.info("Remove directory '{}' success", directory);
        } catch (SftpException e) {
            log.error("Remove directory '{}' failed", directory,  e);
        }
    }

    /**
     * delete file.
     *
     * @param deleteFilePath
     */
    public void deleteFile(String deleteFilePath) throws SftpException {
        checkState();
        sftp.rm(deleteFilePath);
        log.info("delete file {} is delete success", deleteFilePath);
    }

    /**
     * recursive delete file.
     *
     * @param directory
     */
    public void recursiveDeleteFile(String directory) throws SftpException {
        checkState();
        Vector<ChannelSftp.LsEntry> fileAndFolderList = listFiles(directory);
        for (ChannelSftp.LsEntry item : fileAndFolderList) {
            if (!item.getAttrs().isDir()) {
                deleteFile(directory + "/" + item.getFilename());
            } else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
                recursiveDeleteFile(directory + "/" + item.getFilename());
            }
        }
    }

}

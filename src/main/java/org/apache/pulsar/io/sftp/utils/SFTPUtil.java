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


    /**
     * login server
     */
    public void login(){
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
            log.error("Failed to login the sftp server ",e);
        }
    }

    /**
     * close server
     */
    public void logout(){
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
     * List all files in sourcePath
     * @param directory
     * @return
     * @throws SftpException
     */
    public Vector<ChannelSftp.LsEntry> listFiles(String directory) {
        try {
            return sftp.ls(directory);
        } catch (SftpException e) {
            log.error("List files in directory : " + directory + " failed",e);
        }
        return new Vector<>();
    }

    /**
     * rename a file , can be used for remove operation
     * @param oldFilePath
     * @param newFilePath
     * @throws Exception
     */
    public void rename(String oldFilePath, String newFilePath) {
        try {
            sftp.rename(oldFilePath, newFilePath);
            log.info("File: '{}' is rename to '{}' success" , oldFilePath,newFilePath);
        } catch (SftpException e) {
            log.error("Rename file '" + oldFilePath + "' to '" + newFilePath + "' failed",e);
        }
    }

    /**
     * upload file
     * @param directory
     * @param uploadFile
     */
    public void upload(String directory, String uploadFile) {
        try {
            File file = new File(uploadFile);
            sftp.put(Files.newInputStream(file.toPath()), directory + "/" + file.getName());
            log.info("File: '{}' is upload to '{}' success" , uploadFile,directory);
        } catch (SftpException | IOException e) {
            log.error("Upload file '" + uploadFile +  "' to  remote directory '" + directory + "' failed",e);
        }

    }

    /**
     * download file
     * @param directory
     * @param downloadFile
     * @return
     */
    public byte[] download(String directory, String downloadFile) {
        if (directory != null && !"".equals(directory)) {
            try {
                InputStream is = sftp.get(directory + "/" + downloadFile);
                byte[] fileData = IOUtils.toByteArray(is);
                log.info("File: '{}/{}' is download success" ,directory, downloadFile);
                return fileData;
            } catch (SftpException | IOException e) {
                log.error("Download file '" + downloadFile +  "' from  remote directory '" + directory + "' failed",e);
            }
        }
        log.error("Directory is null or blank value,please check!");
        return null;
    }

    /**
     * batch files download
     * @param directory
     * @param fileList
     */
    public void recursiveDownloadFile(String directory,List<byte[]> fileList,Boolean isRecursive) {
        Vector<ChannelSftp.LsEntry> fileAndFolderList = listFiles(directory);
        for (ChannelSftp.LsEntry item : fileAndFolderList) {
            if (!item.getAttrs().isDir()) {
                fileList.add(download(directory,item.getFilename()));
            } else if (!(".".equals(item.getFilename()) || "..".equals(item.getFilename()))) {
                if(isRecursive){
                    recursiveDownloadFile(directory + "/" + item.getFilename(),fileList, true);
                }
            }
        }
    }

    /**
     * directory weather exist
     * @param directory
     * @return
     */
    public boolean isDirExist(String directory) {
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
     * create directory
     * @param directory
     */
    public void createDir(String directory) {
        try {
            sftp.mkdir(directory);
        } catch (SftpException e) {
            log.error("Create directory '" + directory +  "' failed",e);
        }
    }

    /**
     * create multi-level directory
     * @param dirs
     * @param tempPath
     * @param length
     * @param index
     */
    public void createDirIfNotExist(String[] dirs, String tempPath, int length, int index) {
        index++;
        if (index < length) {
            tempPath += "/" + dirs[index];
        }
        try {
            log.info("check directory [" + tempPath + "]");
            sftp.cd(tempPath);
            if (index < length) {
                createDirIfNotExist(dirs, tempPath, length, index);
            }
        } catch (SftpException ex) {
            log.warn("create directory [" + tempPath + "]");
            try {
                sftp.mkdir(tempPath);
                sftp.cd(tempPath);
            } catch (SftpException e) {
                log.error("create directory [" + tempPath + "] failed",e);
            }
            log.info("cd directory [" + tempPath + "]");
            createDirIfNotExist(dirs, tempPath, length, index);
        }
    }

    /**
     * remove directory
     * @param directory
     */
    public void removeDir(String directory) {
        try {
            sftp.rmdir(directory);
        } catch (SftpException e) {
            log.error("Remove directory '" + directory +  "' failed",e);
        }
    }

    /**
     * delete file
     * @param deleteFilePath
     */
    public void deleteFile(String deleteFilePath) {
        try {
            sftp.rm(deleteFilePath);
            log.info("File:{} is delete success" , deleteFilePath);
        } catch (SftpException e) {
            log.error("Delete file : " + deleteFilePath +  " failed",e);
        }
    }


    /**
     * recursive delete file
     * @param directory
     */
    public void recursiveDeleteFile(String directory) {
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

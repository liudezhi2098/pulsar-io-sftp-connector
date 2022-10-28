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
package org.apache.pulsar.io.sftp;

import com.jcraft.jsch.ChannelSftp;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import org.apache.pulsar.io.sftp.utils.FileUtil;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Ignore
public class SFTPUtilTest {

    String username = "sftp_user";
    String password = "12345678";
    String host = "20.120.20.201";


    @Test
    public void listFilesTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        Vector<ChannelSftp.LsEntry> fileList = sftp.listFiles("/sftpdata/fujun/fujun");
        Iterator<ChannelSftp.LsEntry> it = fileList.iterator();
        while (it.hasNext()) {
            ChannelSftp.LsEntry lsEntry = it.next();
            String fileName = lsEntry.getFilename();

            if (".".equals(fileName) || "..".equals(fileName)) {
                continue;
            }
            System.out.println(lsEntry.getFilename());
            System.out.println(lsEntry.getAttrs().getAtimeString());
            System.out.println(lsEntry.getAttrs().getMtimeString());
        }
        sftp.logout();
    }

    @Test
    public void uploadFileTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.upload("/sftpdata/input/common_file", "/Users/fujun/Downloads/fujun-key.pem");
        sftp.logout();
    }

    @Test
    public void renameFileTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.rename("/sftpdata/values-2.10.1.8.yaml", "/sftpdata/fujun/values-2.10.1.8.yaml");
        sftp.logout();
    }

    @Test
    public void isDirExistTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        System.out.println(sftp.isDirExist("/sftpdata/testdir/"));
        System.out.println(sftp.isDirExist("/sftpdata/test"));
        System.out.println(sftp.isDirExist("/sftpdata/testdir/fujun-key.pem"));
        sftp.logout();
    }

    @Test
    public void downloadTest() throws NoSuchAlgorithmException, IOException {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        byte[] byt = sftp.download("/sftpdata/fujun", "values-2.10.1.8.yaml");
        System.out.println(FileUtil.getFileMD5(byt));
        System.out.println(Arrays.toString(byt));
        sftp.logout();
    }

    @Test
    public void recursiveDownloadTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        List<byte[]> fileList = new LinkedList<>();
        sftp.recursiveDownloadFile("/sftpdata", fileList, false);
        System.out.println(fileList.size());
        sftp.logout();
    }

    @Test
    public void createDirTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.createDir("/sftpdata/fujun/test");
        System.out.println("create dir success");
        sftp.logout();
    }

    @Test
    public void createDirIfNotExistTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        String tempPath = "/sftpdata/fujun";
        String path = "/aa/bb";
        System.out.println(path.split("/").length);
        sftp.createDirIfNotExist(path.split("/"), tempPath, path.split("/").length, 0);
        sftp.logout();
    }

    @Test
    public void removeDirTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.removeDir("/sftpdata/fujun");
        System.out.println("remove dir success");
        sftp.logout();
    }

    @Test
    public void deleteFileTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.deleteFile("/sftpdata/fujun/values-2.10.1.8.yaml");
        System.out.println("delete file success");
        sftp.logout();
    }

    @Test
    public void recursiveDeleteTest() {
        SFTPUtil sftp = new SFTPUtil(username, password, host, 22);
        sftp.login();
        sftp.recursiveDeleteFile("/sftpdata/fujun");
        sftp.logout();
    }

}

package org.apache.pulsar.io.sftp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.sftp.source.SFTPSourceConfig;

public class SFTPSourceTest {
    public static void main(String[] args) {
        String host = "20.120.20.201";
        String username = "sftp_user";
        String password = "12345678";
        String inputDirectory = "/sftpdata/read";
        String movedDirectory = "/sftpdata/fujun";
        String illegalFileDirectory = "/sftpdata/testdir";
        Map<String,Object> config = new HashMap<>();
        config.put("host",host);
        config.put("username",username);
        config.put("password",password);
        config.put("inputDirectory",inputDirectory);
        config.put("movedDirectory",movedDirectory);
        config.put("illegalFileDirectory",illegalFileDirectory);
        try {
            SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
            sftpConfig.validate();
            System.out.println(sftpConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

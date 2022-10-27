package org.apache.pulsar.io.sftp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.sftp.source.SFTPSourceConfig;
import org.junit.Ignore;

@Ignore
public class SFTPSourceTest {
    public static void main(String[] args) throws IOException {
        String host = "20.120.20.201";
        String username = "sftp_user";
        String password = "12345678";
        String inputDirectory = "/sftpdata/input";
        String movedDirectory = "/sftpdata/moved";
        String illegalFileDirectory = "/sftpdata/illegal_file";
        Map<String,Object> config = new HashMap<>();
        config.put("host",host);
        config.put("username",username);
        config.put("password",password);
        config.put("inputDirectory",inputDirectory);
        config.put("movedDirectory",movedDirectory);
        config.put("illegalFileDirectory",illegalFileDirectory);
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sftpConfig.validate();
    }
}

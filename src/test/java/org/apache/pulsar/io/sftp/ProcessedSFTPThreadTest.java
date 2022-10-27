package org.apache.pulsar.io.sftp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.io.sftp.source.ProcessedSFTPThread;
import org.apache.pulsar.io.sftp.source.SFTPSourceConfig;
import org.apache.pulsar.io.sftp.source.SFTPSourceRecord;
import org.junit.Ignore;

@Ignore
public class ProcessedSFTPThreadTest {
    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
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
        config.put("keepFile",false);
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sftpConfig.validate();

        BlockingQueue<SFTPSourceRecord> recentlyProcessed = new LinkedBlockingQueue<>();
        for (int i = 0; i < 10; i++) {
            SFTPSourceRecord record = new SFTPSourceRecord("fujun"+i+".txt",("fujun"+i).getBytes(StandardCharsets.UTF_8),".",new Date().toString());
            recentlyProcessed.offer(record);
        }
        Thread t = new ProcessedSFTPThread(sftpConfig,recentlyProcessed);
        t.start();
    }
}

package org.apache.pulsar.io.sftp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.io.sftp.source.SFTPConsumerThread;
import org.apache.pulsar.io.sftp.source.SFTPSource;
import org.apache.pulsar.io.sftp.source.SFTPSourceConfig;
import org.apache.pulsar.io.sftp.source.SFTPSourceRecord;

public class SFTPConsumerThreadTest {
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
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sftpConfig.validate();

        BlockingQueue<SFTPSourceRecord> workQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < 10; i++) {
            SFTPSourceRecord record = new SFTPSourceRecord("fujun"+i+".txt",("fujun"+i).getBytes(StandardCharsets.UTF_8),"",new Date().toString());
            workQueue.offer(record);
        }
        BlockingQueue<SFTPSourceRecord> inProcess = new LinkedBlockingQueue<>();
        BlockingQueue<SFTPSourceRecord> recentlyProcessed = new LinkedBlockingQueue<>();
        SFTPSource source = new SFTPSource();

        Thread t = new SFTPConsumerThread(source,workQueue,inProcess,recentlyProcessed);
        t.start();
    }
}

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


import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.sftp.source.SFTPListingThread;
import org.apache.pulsar.io.sftp.source.SFTPSource;
import org.apache.pulsar.io.sftp.source.SFTPSourceConfig;
import org.testng.annotations.Ignore;

@Ignore
public class SFTPListingThreadTest {

    public static void main(String[] args) throws Exception {
        String host = "20.120.20.201";
        String username = "sftp_user";
        String password = "12345678";
        String inputDirectory = "/sftpdata/input";
        String movedDirectory = "/sftpdata/moved";
        String illegalFileDirectory = "/sftpdata/illegal_file";
        Map<String, Object> config = new HashMap<>();
        config.put("host", host);
        config.put("username", username);
        config.put("password", password);
        config.put("inputDirectory", inputDirectory);
        config.put("movedDirectory", movedDirectory);
        config.put("illegalFileDirectory", illegalFileDirectory);
        SFTPSourceConfig sftpConfig = SFTPSourceConfig.load(config);
        sftpConfig.validate();
        SFTPSource source = new SFTPSource();
        source.setSFTPSourceConfig(sftpConfig);
        Thread t = new SFTPListingThread(source);
        t.start();
    }
}

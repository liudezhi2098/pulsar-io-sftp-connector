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
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.io.sftp.source.SFTPSource;

public class SFTPSourceLocalRunner {
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
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

        String brokerUrl = "pulsar://127.0.0.1:6650";
        String topic = "sftp_file_source_test";
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setName("sftp_file_source");
        sourceConfig.setClassName(SFTPSource.class.getName());
        sourceConfig.setTenant("public");
        sourceConfig.setNamespace("default");
        sourceConfig.setConfigs(config);
        sourceConfig.setTopicName(topic);
        LocalRunner runner = LocalRunner.builder().sourceConfig(sourceConfig).brokerServiceUrl(brokerUrl).build();
        runner.start(false);

    }
}

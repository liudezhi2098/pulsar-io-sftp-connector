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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.io.sftp.sink.FileSink;
import org.apache.pulsar.io.sftp.sink.OBSSink;

public class OBSSinkLocalRunner {

    private static final String endPoint = "https://obs.cn-south-1.myhuaweicloud.com";
    private static final String ak = "SLEAG2U7NLRPKPXKCKKF";
    private static final String sk = "4fwVtIOgnFgfJOC56einSewKYZh6sknG66t36FhJ";
    private static final String bucket = "sn-poc-test";

    public static void main(String[] args) throws Exception {

        String brokerUrl = "pulsar://20.231.205.235:6650";
        String topic = "obs_sink_test";

        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName("parquet_file_obs_sink");
        sinkConfig.setClassName(OBSSink.class.getName());
        sinkConfig.setTenant("public");
        sinkConfig.setNamespace("default");
        sinkConfig.setInputs(Arrays.asList(topic));
        sinkConfig.setSourceSubscriptionName("obs_sink_sub");

        Map<String, Object> conf = new HashMap<>();
        conf.put("outDirectory", "/obs_test/parquet");
        conf.put("parquetWriterVersion", "v2");
        conf.put("parquetWriterMode", "create");
        conf.put("fileWriteClass","org.apache.pulsar.io.sftp.sink.MessageToParquetFileOBSWriter");
        //conf.put("fileWriteClass", "org.apache.pulsar.io.sftp.sink.MessageToRawFileWriter");

        conf.put("endPoint", endPoint);
        conf.put("accessKey", ak);
        conf.put("secretKey", sk);
        conf.put("bucket", bucket);
        sinkConfig.setConfigs(conf);

        LocalRunner runner = LocalRunner.builder().sinkConfig(sinkConfig).brokerServiceUrl(brokerUrl).build();
        runner.start(false);

    }
}

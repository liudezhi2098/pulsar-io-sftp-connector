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

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import org.apache.pulsar.io.sftp.utils.HWObsUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OBSUtilsTest {

    private static final String endPoint = "https://obs.cn-south-1.myhuaweicloud.com";
    private static final String ak = "SLEAG2U7NLRPKPXKCKKF";
    private static final String sk = "4fwVtIOgnFgfJOC56einSewKYZh6sknG66t36FhJ";
    private static final String bucket = "sn-poc-test";
    private ObsClient obsClient;

    @BeforeTest
    public void before() {
        ObsConfiguration conf = new ObsConfiguration();
        conf.setEndPoint(endPoint);
        obsClient = HWObsUtil.getObsClient(ak, sk, null, conf);
    }

    @AfterTest
    public void after() {
        try {
            obsClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void listBucketsTest() {
        System.out.println(obsClient.listBuckets());
    }

    @Test
    public void createFolderTest() {
        final String keySuffixWithSlash = "obs_test/";
        obsClient.putObject(bucket, keySuffixWithSlash, new ByteArrayInputStream(new byte[0]));
    }

    @Test
    public void uploadObjectTest() {
        obsClient.putObject(bucket, "obs_test/parquet/file.parquet",
                new File("/Users/fujun/Downloads/obs_test/parquet/file.parquet"));
    }

    @Test
    public void uploadTempObjectTest() {
        obsClient.putObject(bucket, "obs_test/hello.txt", new ByteArrayInputStream("Hello OBS".getBytes()));
    }

}

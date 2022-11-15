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
package org.apache.pulsar.io.sftp.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.PutObjectResult;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.apache.pulsar.io.sftp.utils.HWObsUtil;

/**
 * Write files to local disk in parquet format.
 */
@Slf4j
public class MessageToParquetFileOBSWriter implements MessageOBSWriter<byte[]> {

    @Override
    public void writeToStorage(Record<byte[]> record, OBSSink obsSink, ObsConfiguration conf) {
        OBSSinkConfig sinkConfig = obsSink.getOBSSinkConfig();
        String outDirectory = sinkConfig.getOutDirectory();
        //TODO
        String messageKey = record.getKey().get();
        String messageValue = new String(record.getValue(), StandardCharsets.UTF_8);
        String parquetFileName = buildParquetFileName(messageKey, messageValue);
        // The file is temporarily stored in the server's directory and will be deleted after uploading obs
        String tempParquetFilePath = "/tmp" + outDirectory + "/" + parquetFileName;
        ParquetFileWriter.Mode mode;
        if ("create".equals(sinkConfig.getParquetWriterMode())) {
            mode = ParquetFileWriter.Mode.CREATE;
        } else if ("overwrite".equals(sinkConfig.getParquetWriterMode())) {
            mode = org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
        } else {
            throw new IllegalStateException(
                    "Illegal config [parquetWriterMode] : " + sinkConfig.getParquetWriterMode() + " !");
        }
        ParquetWriter writer = null;
        try {
            MessageType schema = MessageTypeParser.parseMessageType(Constants.PARQUET_SCHEMA);
            Configuration configuration = new Configuration();
            GroupWriteSupport.setSchema(schema, configuration);
            GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
            groupWriteSupport.init(configuration);
            writer = ExampleParquetWriter.builder(new Path(tempParquetFilePath))
                    .withType(schema)
                    .withConf(configuration)
                    .withPageSize(sinkConfig.getDefaultPageSize())
                    .withDictionaryPageSize(sinkConfig.getDictionaryPageSize())
                    .withDictionaryEncoding(sinkConfig.getEnableDictionary())
                    .withValidation(sinkConfig.getEnableValidation())
                    .withRowGroupSize(sinkConfig.getDefaultBlockSize())
                    .withMaxPaddingSize(sinkConfig.getMaxPaddingSize())
                    .withWriterVersion(ParquetProperties.WriterVersion.fromString(sinkConfig.getParquetWriterVersion()))
                    .withCompressionCodec(CompressionCodecName.fromConf(sinkConfig.getCompressionCodecName()))
                    .withWriteMode(mode)
                    .build();
            SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
            Group group = simpleGroupFactory.newGroup();
            group.add(Constants.ID, record.getMessage().get().getSequenceId());
            group.add(Constants.TOPIC, record.getTopicName().get());
            group.add(Constants.MESSAGE, messageValue);
            group.add(Constants.CREATE_TIME, new Date().getTime());
            writer.write(group);
            writer.close();

            //upload to obs bucket & delete temp file
            File file = new File(tempParquetFilePath);
            ObsClient obsClient = HWObsUtil.getObsClient(sinkConfig.getAccessKey(), sinkConfig.getSecretKey(),
                    sinkConfig.getSecurityToken(), conf);
            PutObjectRequest request = new PutObjectRequest();
            request.setFile(file);
            request.setBucketName(sinkConfig.getBucket());
            request.setObjectKey((outDirectory + "/" + parquetFileName).replaceFirst("/", ""));
            request.setExpires(sinkConfig.getExpires());
            PutObjectResult result = obsClient.putObject(request);
            log.info("Put Object to OBS success , Path : " + result.getObjectUrl());
            file.delete();
        } catch (IOException e) {
            log.error("Write parquet file error", e);
        }
    }

    private String buildParquetFileName(String messageKey, String messageValue) {
        StringBuilder builder = new StringBuilder();
        if (JSONObject.isValidObject(messageValue)) {
            JSONObject content = JSON.parseObject(messageValue);
            if (content.containsKey("databaseName")) {
                String database = content.getString("databaseName");
                builder.append(database).append("-");
            }
            if (content.containsKey("source")) {
                JSONObject source = content.getJSONObject("source");
                if (source.containsKey("table")) {
                    String table = source.getString("table");
                    builder.append(table).append("-");
                }
            }
        } else if (StringUtils.isNotBlank(messageKey)) {
            builder.append(messageKey).append("-");
        }
        return builder.append(new Date().getTime()).append(".parquet").toString();
    }
}

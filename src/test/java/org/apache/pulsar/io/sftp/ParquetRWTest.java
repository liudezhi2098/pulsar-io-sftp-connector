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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.pulsar.io.sftp.sink.FileSinkConfig;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.testng.annotations.Test;


public class ParquetRWTest {

    @Test
    public void writeToParquet() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("outDirectory", "/Users/fujun/Desktop");
        //conf.put("compressionCodecName","/Users/fujun/Desktop");
        conf.put("parquetWriterVersion", "v2");
        conf.put("parquetWriterMode", "overwrite");
        conf.put("fileWriteClass", "org.apache.pulsar.io.sftp.sink.MessageToParquetFileWriter");
        FileSinkConfig sinkConfig = FileSinkConfig.load(conf);
        sinkConfig.validate();

        String outDirectory = sinkConfig.getOutDirectory();
        String parquetFileName = "file.parquet";
        String parquetFilePath = outDirectory + "/" + parquetFileName;
        ParquetFileWriter.Mode mode;
        if ("create".equals(sinkConfig.getParquetWriterMode())) {
            mode = ParquetFileWriter.Mode.CREATE;
        } else if ("overwrite".equals(sinkConfig.getParquetWriterMode())) {
            mode = ParquetFileWriter.Mode.OVERWRITE;
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
            writer = ExampleParquetWriter.builder(new Path(parquetFilePath))
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
            group.add(Constants.ID, UUID.randomUUID().toString());
            group.add(Constants.TOPIC, "jun_test");
            group.add(Constants.MESSAGE, "{\"before\":null,\"after\":{\"id\":5,\"name\":\"我\",\"sex\":\"man\","
                    + "\"city\":null},\"source\":{\"version\":\"1.7.1.Final\",\"connector\":\"mysql\","
                    + "\"name\":\"test4\",\"ts_ms\":1667375397000,\"snapshot\":\"false\",\"db\":\"test_1\","
                    + "\"sequence\":null,\"table\":\"t1\",\"server_id\":1000,\"gtid\":null,\"file\":\"mysql-bin"
                    + ".000003\",\"pos\":2987,\"row\":0,\"thread\":null,\"query\":\"INSERT INTO `test_1`.`t1`(`id`, "
                    + "`name`, `sex`,`city`) VALUES (5, '我', 'man','')\"},\"op\":\"c\",\"ts_ms\":1667375397457,"
                    + "\"transaction\":null}");
            group.add(Constants.CREATE_TIME, new Date().getTime());
            writer.write(group);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void readFormParquet() throws IOException {
        Configuration configuration = new Configuration();
        String schemaName = Constants.SCHEMA_NAME;
        String filePath = "/Users/fujun/Desktop/file.parquet";

        // set filter
        //ParquetInputFormat.setFilterPredicate(configuration, lt(longColumn("id"), (long)(5)));
        FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);

        // set schema
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, Constants.ID));
        builder.addField(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Constants.MESSAGE));
        builder.addField(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Constants.TOPIC));
        builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64,
                Constants.CREATE_TIME));
        MessageType querySchema = builder.named(schemaName);
        System.out.println("******querySchema.toString()********* : " + querySchema.toString());
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

        // set reader, withConf set specific fields (requested projection), withFilter set the filter.
        // if omit withConf, it queries all fields
        ParquetReader.Builder<Group> reader = ParquetReader
                .builder(new GroupReadSupport(), new Path(filePath))
                .withConf(configuration)
                .withFilter(filter);

        // read
        ParquetReader<Group> build = reader.build();
        Group line;
        while ((line = build.read()) != null) {
            System.out.println(line);
        }

        File file = new File(filePath);
        file.delete();
    }


}

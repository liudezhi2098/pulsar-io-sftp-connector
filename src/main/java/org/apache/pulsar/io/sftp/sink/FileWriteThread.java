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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.sftp.utils.Constants;

/**
 * Worker thread that consumes the contents of the files
 * and publishes them to a Pulsar topic.
 */
@Slf4j
public class FileWriteThread extends Thread {

    private final FileSink sink;

    public FileWriteThread(FileSink sink) {
        this.sink = sink;
    }

    public void run() {
        try {
            while (true) {
                Record<byte[]> record = sink.getQueue().take();
                try {
                    FileSinkConfig sinkConf = sink.getFileSinkConfig();
                    writeToParquetFile(record,sinkConf);
                    //writeToFile(record);
                    record.ack();
                } catch (Exception e) {
                    record.fail();
                    log.error("FileWriteThread run error", e);
                }
            }
        } catch (InterruptedException e) {
            // just terminate
        }
    }

    private void writeToParquetFile(Record<byte[]> record,FileSinkConfig sinkConfig)  {

        String outDirectory = sinkConfig.getOutDirectory();
        //todo
        String parquetFileName = "file.parquet";
        String parquetFilePath = outDirectory + "/" + parquetFileName;
        ParquetFileWriter.Mode mode;
        if("create".equals(sinkConfig.getParquetWriterMode())){
            mode = ParquetFileWriter.Mode.CREATE;
        } else if("overwrite".equals(sinkConfig.getParquetWriterMode())){
            mode = ParquetFileWriter.Mode.OVERWRITE;
        } else {
            throw new IllegalStateException("Illegal config [parquetWriterMode] : " + sinkConfig.getParquetWriterMode() + " !");
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
            group.add(Constants.ID, record.getMessage().get().getSequenceId());
            group.add(Constants.TOPIC, record.getTopicName().get());
            group.add(Constants.MESSAGE, new String(record.getValue(), StandardCharsets.UTF_8));
            group.add(Constants.CREATE_TIME, 0);
            writer.write(group);

        } catch (IOException e) {
            log.error("Write parquet file error", e);
        } finally {
            try {
                if(writer != null){
                    writer.close();
                }
            } catch (IOException e) {
                log.error("Close ParquetWriter error", e);
            }
        }


    }

    private void writeToFile(Record<byte[]> record) throws IOException {
        RandomAccessFile randomFile = null;
        try {
            Message<byte[]> msg = record.getMessage().get();
            byte[] contents = msg.getValue();
            String name = new File(msg.getProperty(Constants.FILE_NAME)).getName();
            String fileName = sink.getFileSinkConfig().getOutDirectory() + "/" + name;
            String md5 = msg.getProperty(Constants.FILE_MD5);
            File writeFile = new File(fileName);
            if (writeFile.exists()) {
                writeFile.delete();
            }
            randomFile = new RandomAccessFile(fileName, "rw");
            randomFile.seek(randomFile.length());
            randomFile.write(contents);
            randomFile.close();
            randomFile = null;
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

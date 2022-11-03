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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * Configuration class for the File Sink Connector.
 */
@Data
@Accessors(chain = true)
public class FileSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final List<String> parquetWriterVersionList = Arrays.asList("v1","v2");

    private static final List<String> parquetWriterModeList= Arrays.asList("create","overwrite");

    private static final List<String>
            compressionCodecNameList = Arrays.asList("SNAPPY","LZO","BROTLI","LZ4","GZIP","ZSTD");

    /**
     * The output directory from which to pull files.
     */
    private String outDirectory;

    /**
     * The number of worker threads that will be processing the files.
     */
    private Integer numWorkers = 1;

    /**
     * The logic that generates the message to the file.
     * MessageToRawFileWriter : Single message generates raw file.
     * MessageToParquetFileWriter : Single message as one record in parquet file.
     */
    private String fileWriteClass = "org.apache.pulsar.io.sftp.sink.MessageToParquetFileWriter";

    //parquet config version
    /**
     * Parquet Writer Version,"v1" or "v2".
     */
    private String parquetWriterVersion = "v2";

    /**
     * Parquet Writer Mode,"create" or "overwrite".
     */
    private String parquetWriterMode = "create";

    /**
     * Compression Codec Name,null is UNCOMPRESSED,also can SNAPPY,GZIP,LZO,BROTLI,LZ4,ZSTD.
     */
    private String compressionCodecName = null;

    /**
     * Enable or disable dictionary encoding for the constructed writer.
     */
    private Boolean enableDictionary = Boolean.TRUE;

    /**
     * Enable or disable validation for the constructed writer.
     */
    private Boolean enableValidation = Boolean.TRUE;

    /**
     * Set the Parquet format page size used by the constructed writer.
     */
    private Integer defaultPageSize = ParquetProperties.DEFAULT_PAGE_SIZE;

    /**
     * Set the Parquet format dictionary page size used by the constructed writer.
     */
    private Integer dictionaryPageSize = ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE;

    /**
     * Set the maximum amount of padding, in bytes, that will be used to align row groups with blocks in the underlying filesystem.
     * If the underlying filesystem is not a block filesystem like HDFS, this has no effect.
     */
    private Integer maxPaddingSize  = ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

    /**
     * Set the Parquet format row group size used by the constructed writer.
     */
    private Long defaultBlockSize = 134217728L;


    public static FileSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), FileSinkConfig.class);
    }

    public static FileSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), FileSinkConfig.class);
    }


    public void validate() {
        if (StringUtils.isBlank(outDirectory)) {
            throw new IllegalArgumentException("Required property not set.");
        } else if (outDirectory.endsWith("/")) {
            throw new IllegalArgumentException("Specified illegal out directory : '" + outDirectory + "'  cannot end with '/'");
        }

        if (numWorkers != null && numWorkers <= 0) {
            throw new IllegalArgumentException("The property numWorkers must be greater than zero");
        }

        if (defaultPageSize != null && defaultPageSize <= 0) {
            throw new IllegalArgumentException("The property defaultPageSize must be greater than zero");
        }

        if (dictionaryPageSize != null && dictionaryPageSize <= 0) {
            throw new IllegalArgumentException("The property dictionaryPageSize must be greater than zero");
        }

        if (maxPaddingSize != null && maxPaddingSize <= 0) {
            throw new IllegalArgumentException("The property maxPaddingSize must be greater than zero");
        }

        if (defaultBlockSize != null && defaultBlockSize <= 0) {
            throw new IllegalArgumentException("The property numWorkers must be greater than zero");
        }

        if (StringUtils.isNotBlank(compressionCodecName) && !compressionCodecNameList.contains(compressionCodecName)) {
            throw new IllegalArgumentException("Invalid property provided for compressionCodecName : " + compressionCodecName + " , if set must be include in : " + compressionCodecNameList);
        }

        if (StringUtils.isNotBlank(parquetWriterVersion) && !parquetWriterVersionList.contains(parquetWriterVersion)) {
            throw new IllegalArgumentException("Invalid property provided for parquetWriterVersion : " + parquetWriterVersion + " , must be include in : " + parquetWriterVersionList);
        }

        if (StringUtils.isNotBlank(parquetWriterMode) && !parquetWriterModeList.contains(parquetWriterMode)) {
            throw new IllegalArgumentException("Invalid property provided for parquetWriterMode : " + parquetWriterMode + " , must be include in : " + parquetWriterModeList);
        }

        if (StringUtils.isNotBlank(fileWriteClass)) {
            try {
                Class.forName(fileWriteClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }

    }
}
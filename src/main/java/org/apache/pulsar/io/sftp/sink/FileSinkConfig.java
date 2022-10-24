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
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration class for the File Source Connector.
 */
@Data
@Accessors(chain = true)
public class FileSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The output directory from which to pull files.
     */
    private String outDirectory;

    /**
     * The number of worker threads that will be processing the files.
     * This allows you to process a larger number of files concurrently.
     * However, setting this to a value greater than 1 will result in the data
     * from multiple files being "intermingled" in the target topic.
     */
    private Integer numWorkers = 1;


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
        }

        if (numWorkers != null && numWorkers <= 0) {
            throw new IllegalArgumentException("The property numWorkers must be greater than zero");
        }

    }
}
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
package org.apache.pulsar.io.sftp.source;

import static org.apache.pulsar.io.sftp.utils.Constants.FILE_ABSOLUTE_PATH;
import static org.apache.pulsar.io.sftp.utils.Constants.FILE_MD5;
import static org.apache.pulsar.io.sftp.utils.Constants.FILE_MODIFIED_TIME;
import static org.apache.pulsar.io.sftp.utils.Constants.FILE_NAME;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.sftp.utils.Constants;
import org.apache.pulsar.io.sftp.utils.FileUtil;

/**
 * Implementation of the Record interface for File Source data.
 *    - The key is set to the source file name of the record.
 *    - The value is set to the file contents (in bytes)
 *    - The following user properties are also set:
 *      - The source file name
 *      - The absolute path of the source file
 *      - The md5 value of the source file.
 *      - The last modified time of the source file.
 */
@Data
public class SFTPSourceRecord implements Record<byte[]> {

    private final Optional<String> key;
    private final byte[] value;
    private final HashMap<String, String> userProperties = new HashMap<String, String>();

    public SFTPSourceRecord(String fileName, byte[] byt, String absolutePath, String realAbsolutePath,
                            String modifiedTime) throws NoSuchAlgorithmException, IOException {
        this.key = Optional.of(fileName);
        this.value = byt;
        this.setProperty(FILE_NAME, fileName);
        this.setProperty(FILE_ABSOLUTE_PATH, absolutePath);
        this.setProperty(Constants.FILE_ABSOLUTE_PATH, StringUtils.isBlank(realAbsolutePath) ? "." : realAbsolutePath);
        this.setProperty(FILE_MODIFIED_TIME, modifiedTime);
        this.setProperty(FILE_MD5, FileUtil.getFileMD5(byt));
    }

    @Override
    public Optional<String> getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    public Map<String, String> getProperties() {
        return userProperties;
    }

    public void setProperty(String key, String value) {
        userProperties.put(key, value);
    }
}
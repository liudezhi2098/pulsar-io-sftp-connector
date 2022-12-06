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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.sftp.utils.SFTPUtil;

/**
 * Configuration class for the SFTP Source Connector.
 */
@Data
@Accessors(chain = true)
public class SFTPSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * sftp server host.
     */
    private String host;

    /**
     * sftp server port.
     */
    private int port = 22;

    /**
     * sftp server login username.
     */
    private String username;

    /**
     * sftp server login password.
     */
    private String password;

    /**
     * sftp server login privateKey.
     */
    private String privateKey;

    /**
     * The input directory from which to pull files.
     */
    private String inputDirectory;

    /**
     * The pulled files will be moved to this directory.
     */
    private String movedDirectory;

    /**
     * If the file size greater than "maximumSize" and the file is illegal will be moved to this directory.
     */
    private String illegalFileDirectory;

    /**
     * Indicates whether or not to pull files from sub-directories.
     */
    private Boolean recurse = Boolean.TRUE;

    /**
     * If true, the file is not deleted after it has been processed and
     * move to "movedDirectory".
     */
    private Boolean keepFile = Boolean.TRUE;

    /**
     * Only files whose names match the given regular expression will be picked up.
     */
    private String fileFilter = "[^.].*";

    /**
     * The maximum size (in Bytes) that a file can be in order to be processed.
     */
    private Long maximumSize = 1048576L;

    /**
     * The maxi num  file  that a sftp listing can be in order to be processed.
     */
    private int maxFileNumListing = 100;

    /**
     * Indicates whether or not hidden files should be ignored or not.
     */
    private Boolean ignoreHiddenFiles = Boolean.TRUE;

    /**
     * Indicates how long to wait before performing a directory listing.
     */
    private Long pollingInterval = 10000L;

    /**
     * The number of worker threads that will be processing the files.
     * This allows you to process a larger number of files concurrently.
     * However, setting this to a value greater than 1 will result in the data
     * from multiple files being "intermingled" in the target topic.
     */
    private Integer numWorkers = 1;

    /**
     * Used to distribute synchronization tasks,
     * The producer has only one instance listening to the directory through WaitForExclusive mode.
     */
    private String sftpTaskTopic;

    /**
     * record task progress topic name.
     */
    private String taskProgressTopic = "taskProgress";

    /**
     * synchronization tasks subscript name.
     */
    private String sftpTaskTopicSubscriptionName;


    public static SFTPSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), SFTPSourceConfig.class);
    }

    public static SFTPSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SFTPSourceConfig.class);
    }

    public void validate() {

        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("Required property host not set.");
        }

        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Required property username not set.");
        }

        if (StringUtils.isBlank(password) && StringUtils.isBlank(privateKey)) {
            throw new IllegalArgumentException("Property password & privateKey must be set one.");
        }

        if (StringUtils.isNotBlank(password) && StringUtils.isNotBlank(privateKey)) {
            throw new IllegalArgumentException("Property password & privateKey cannot be both set.");
        }


        if (StringUtils.isBlank(taskProgressTopic)) {
            throw new IllegalArgumentException("Required property taskProgressTopic can not empty.");
        }

        if (maximumSize != null && maximumSize < 0) {
            throw new IllegalArgumentException("The property maximumSize must be non-negative");
        }

        if (pollingInterval != null && pollingInterval <= 0) {
            throw new IllegalArgumentException("The property pollingInterval must be greater than zero");
        }

        if (numWorkers != null && numWorkers <= 0) {
            throw new IllegalArgumentException("The property numWorkers must be greater than zero");
        }

        if (StringUtils.isNotBlank(fileFilter)) {
            try {
                Pattern.compile(fileFilter);
            } catch (final PatternSyntaxException psEx) {
                throw new IllegalArgumentException("Invalid Regex pattern provided for fileFilter");
            }
        }

        if (StringUtils.isBlank(inputDirectory)) {
            throw new IllegalArgumentException("Required property inputDirectory not set.");
        } else if (isLegalSuffix(inputDirectory)) {
            throw new IllegalArgumentException(
                    "Specified input directory : '" + inputDirectory + "'  cannot end with '/'");
        } else if (!isSftpDirExist(inputDirectory)) {
            throw new IllegalArgumentException(
                    "Specified input directory : '" + inputDirectory + "'  does not exist in sftp server.");
        }

        if (StringUtils.isBlank(movedDirectory)) {
            throw new IllegalArgumentException("Required property movedDirectory not set.");
        } else if (movedDirectory.startsWith(inputDirectory)) {
            throw new IllegalArgumentException("Specified moved directory : '" + movedDirectory
                    + "' cannot be a subdirectory of input directory : '" + inputDirectory + "'");
        } else if (isLegalSuffix(movedDirectory)) {
            throw new IllegalArgumentException(
                    "Specified moved directory : '" + movedDirectory + "'  cannot end with '/'");
        } else if (!isSftpDirExist(movedDirectory)) {
            throw new IllegalArgumentException(
                    "Specified moved directory : '" + movedDirectory + "'  does not exist in sftp server.");
        }

        if (StringUtils.isBlank(illegalFileDirectory)) {
            throw new IllegalArgumentException("Required property illegalFileDirectory not set.");
        } else if (illegalFileDirectory.startsWith(inputDirectory)) {
            throw new IllegalArgumentException("Specified illegal file directory : '" + illegalFileDirectory
                    + "' cannot be a subdirectory of input directory : '" + inputDirectory + "'");
        } else if (isLegalSuffix(illegalFileDirectory)) {
            throw new IllegalArgumentException(
                    "Specified illegal file directory : '" + illegalFileDirectory + "'  cannot end with '/'");
        } else if (Objects.equals(illegalFileDirectory, movedDirectory)) {
            throw new IllegalArgumentException(
                    "moved directory and illegal file directory cannot be same : '" + movedDirectory + "'");
        } else if (!isSftpDirExist(illegalFileDirectory)) {
            throw new IllegalArgumentException("Specified illegal file directory : '" + illegalFileDirectory
                    + "'  does not exist in sftp server.");
        }

    }

    private Boolean isSftpDirExist(String directory) {
        SFTPUtil sftp;
        if (StringUtils.isNotBlank(password)) {
            sftp = new SFTPUtil(username, password, host, port);
        } else {
            sftp = new SFTPUtil(username, host, port, privateKey);
        }
        sftp.login();
        Boolean exist = sftp.isDirExist(directory);
        sftp.logout();
        return exist;
    }

    private Boolean isLegalSuffix(String directory) {
        if (!"/".equals(directory)) {
            return directory.endsWith("/");
        }
        return false;
    }
}
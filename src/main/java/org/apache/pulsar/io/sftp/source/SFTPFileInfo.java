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

import java.util.Objects;
import lombok.Data;
import org.apache.pulsar.io.sftp.common.SFTPTaskState;

@Data
public class SFTPFileInfo {

    private final String fileName;
    private final String directory;
    private final String realAbsolutePath;
    private final String modifiedTime;
    private SFTPTaskState state = SFTPTaskState.None;

    public SFTPFileInfo(String fileName, String directory, String realAbsolutePath, String modifiedTime) {
        this.fileName = fileName;
        this.directory = directory;
        this.realAbsolutePath = realAbsolutePath;
        this.modifiedTime = modifiedTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof SFTPFileInfo)) {
            return false;
        }
        SFTPFileInfo fileInfo = (SFTPFileInfo) obj;
        return fileInfo.fileName.equals(this.fileName)
                && fileInfo.directory.equals(this.directory)
                && fileInfo.realAbsolutePath.equals(this.realAbsolutePath)
                && fileInfo.modifiedTime.equals(this.modifiedTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, directory, realAbsolutePath, modifiedTime);
    }

}
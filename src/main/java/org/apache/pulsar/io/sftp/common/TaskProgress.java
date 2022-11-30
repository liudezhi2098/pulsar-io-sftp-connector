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
package org.apache.pulsar.io.sftp.common;

import java.util.HashMap;
import java.util.Objects;
import lombok.Data;

@Data
public class TaskProgress {
    private static final long serialVersionUID = 1L;
    private String taskId;
    private String taskName;
    private String taskType;
    private String taskCategory;
    private int timestamp;
    private TaskState state = TaskState.None;
    private final HashMap<String, String> taskProperties = new HashMap<String, String>();

    public TaskProgress() {
    }

    public TaskProgress(String taskId, String taskCategory, String taskType, String taskName) {
        this.taskId = taskId;
        this.taskCategory = taskCategory;
        this.taskType = taskType;
        this.taskName = taskName;
    }

    public void setProperty(String key, String value) {
        taskProperties.put(key, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof TaskProgress)) {
            return false;
        }
        TaskProgress fileInfo = (TaskProgress) obj;
        return fileInfo.taskId.equals(this.taskId)
                && fileInfo.taskType.equals(this.taskType)
                && fileInfo.taskName.equals(this.taskName)
                && fileInfo.taskCategory.equals(this.taskCategory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, taskType, taskCategory);
    }
}
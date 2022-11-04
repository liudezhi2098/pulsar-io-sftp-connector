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
package org.apache.pulsar.io.sftp.utils;

public class Constants {

    public static final String FILE_NAME = "file.name";
    public static final String FILE_MD5 = "file.md5";
    public static final String FILE_PATH = "file.path";
    public static final String FILE_ABSOLUTE_PATH = "file.path.absolute";
    public static final String FILE_MODIFIED_TIME = "file.modified.time";

    public static final String TASK_PROGRESS_SFTP = "task.progress.sftp";
    public static final String TASK_PROGRESS_SOURCE_TYPE = "source";
    public static final String TASK_PROGRESS_SINK_TYPE = "sink";

    public static final String SCHEMA_NAME  = "Record";
    public static final String ID  = "id";
    public static final String TOPIC  = "topic";
    public static final String MESSAGE  = "message";
    public static final String CREATE_TIME  = "create_time";
    public static final String PARQUET_SCHEMA  = "message " + SCHEMA_NAME + " {\n"
            + "  optional int64 " + ID + ";\n"
            + "  optional binary " + TOPIC + ";\n"
            + "  optional binary " + MESSAGE + ";\n"
            + "  optional int64 " + CREATE_TIME + ";\n"
            + "}";


}

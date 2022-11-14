# Overview
This is a sftp io connector source for [pulsar](https://github.com/apache/pulsar).
It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.
This Connector is used to write each file in the specified directory on the sftp server into pulsar in the form of a
message.

# Architecture
![image](https://user-images.githubusercontent.com/33149602/199865567-43776801-f120-4f83-a17e-4d9e0d38e4df.png)

# TaskProgress

|    Settings    | Output type |                Explanation                 |
|:--------------:|:-----------:|:------------------------------------------:|
|     taskId     |   string    |                   taskId                   |
|    taskType    |   string    |      taskType, such as : source,sink       |
|  taskCategory  |   string    | taskCategory, such as : task.progress.sftp |
|   timestamp    |     int     |                 timestamp                  |
| taskProperties |   string    |    taskProperties  Map<String, String>     |
|     state      |   string    |      state, such as : Success, Failed      |

# Pulsar Sftp Source

### Pulsar Sftp Source Configuration Options
|           Settings            |                  Output type                  | Required  | Explanation                                                                                          |
|:-----------------------------:|:---------------------------------------------:|:---------:|:-----------------------------------------------------------------------------------------------------|
|             host              |                    string                     |    Yes    | Sftp server host                                                                                     |
|             port              |               int,default is 22               |    No     | Sftp server port                                                                                     |
|           username            |                    string                     |    Yes    | Sftp server username                                                                                 |
|           password            |                    string                     |    No     | Sftp server password                                                                                 |
|          privateKey           |                    string                     |    No     | Sftp server privateKey                                                                               |
|        inputDirectory         |                    string                     |    Yes    | The input directory from which to pull files.                                                        |
|        movedDirectory         |                    string                     |    Yes    | The pulled files will be moved to this directory.                                                    |
|     illegalFileDirectory      |                    string                     |    Yes    | If the file size greater than "maximumSize" and the file is illegal will be moved to this directory. |
|            recurse            | boolean, one of [true, false].default is true |    No     | Indicates whether or not to pull files from sub-directories.                                         |
|           keepFile            | boolean, one of [true, false].default is true |    No     | The file is not deleted after it has been processed and move to "movedDirectory".                    |
|          fileFilter           |          string,default is "[^.].*"           |    No     | Only files whose names match the given regular expression will be picked up.                         |
|          maximumSize          |           Long,default is 1048576L            |    No     | The maximum size (in Bytes) that a file can be in order to be processed.                             |
|       maxFileNumListing       |              int,default is 100               |    No     | The maxi num  file  that a sftp listing can be in order to be processed.                             |
|       ignoreHiddenFiles       | boolean, one of [true, false].default is true |    No     | Indicates whether or not hidden files should be ignored or not.                                      |
|        pollingInterval        |            Long,default is 10000L             |    No     | Indicates how long to wait before performing a directory listing.                                    |
|          numWorkers           |               int,default is 1                |    No     | The number of worker threads that will be processing the files.                                      |
|         sftpTaskTopic         |         string,default is "sftp_task"         |    No     | Synchronization tasks topic name.                                                                    |
| sftpTaskTopicSubscriptionName |       string,default is "sftp_task_sub"       |    No     | Synchronization tasks subscript name.                                                                |
|       taskProgressTopic       |       string,default is "taskProgress"        |    No     | record task progress topic name.                                                                     |

### Example
sftp-source-config.yaml
```
configs:
 host: "127.0.0.1"
 username: "root"
 password: "12345678"
 inputDirectory: "/sftpdata/input"
 movedDirectory: "/sftpdata/moved"
 illegalFileDirectory: "/sftpdata/illegal_file"
 taskProgressTopic: taskProgress
 sftpTaskTopic: sftp_task
 sftpTaskTopicSubscriptionName: sftp_task_sub
 numWorkers: 10
```

# Pulsar Huawei OBS Sink

### Pulsar Huawei OBS Sink Configuration Options
|       Settings       |                               Output type                               | Required | Explanation                                                                                                                  |
|:--------------------:|:-----------------------------------------------------------------------:|:--------:|:-----------------------------------------------------------------------------------------------------------------------------|
|     outDirectory     |                                 string                                  |   Yes    | The output directory from which to pull files.                                                                               |
|    fileWriteClass    | string,one of [MessageToParquetFileOBSWriter,MessageToRAWFileOBSWriter] |   Yes    | The logic that generates the message to the file.                                                                            |
| parquetWriterVersion |                          string,one of [v1,v2]                          |    No    | Parquet Writer Version                                                                                                       |
|  parquetWriterMode   |                    string,one of [create,overwrite]                     |    No    | Parquet Writer Mode                                                                                                          |
| compressionCodecName |          string ,one of[null,SNAPPY,GZIP,LZO,BROTLI,LZ4,ZSTD]           |    No    | Compression Codec Name,null is UNCOMPRESSED                                                                                  |
|   enableDictionary   |              boolean, one of [true, false].default is true              |    No    | Enable or disable dictionary encoding for the constructed writer.                                                            |
|   enableValidation   |              boolean, one of [true, false].default is true              |    No    | Enable or disable validation for the constructed writer.                                                                     |
|   defaultPageSize    |                         int,default is 1048576                          |    No    | Set the Parquet format page size used by the constructed writer.                                                             |
|  dictionaryPageSize  |                         int,default is 1048576                          |    No    | Set the Parquet format dictionary page size used by the constructed writer.                                                  |
|    maxPaddingSize    |                     int,default is 8 * 1024 * 1024                      |    No    | Set the maximum amount of padding, in bytes, that will be used to align row groups with blocks in the underlying filesystem. |
|   defaultBlockSize   |                        Long,default is 1048576L                         |    No    | Set the Parquet format row group size used by the constructed writer.                                                        |
|  taskProgressTopic   |                    string ,default is "taskProgress"                    |    No    | synchronization tasks progress topic.                                                                                        |
|       endPoint       |                                 string                                  |   Yes    | obs end point address.                                                                                                       |
|      accessKey       |                                 string                                  |    No    | obs access key.                                                                                                              |
|      secretKey       |                                 string                                  |    No    | obs secret key.                                                                                                              |
|    securityToken     |                                 string                                  |    No    | obs security token.                                                                                                          |
|        bucket        |                                 string                                  |   Yes    | obs bucket.                                                                                                                  |
|       expires        |                            int,default is -1                            |    No    | The expiration time of the object (days). Objects are automatically deleted after expiration.                                |
|  connectionTimeout   |                          int,default is 60000                           |    No    | Timeout for establishing HTTP/HTTPS connections. Defaults to 60000 milliseconds.                                             |
|    socketTimeout     |                          int,default is 60000                           |    No    | The timeout period for the Socket layer to transmit data. Defaults to 60000 milliseconds.                                    |
|  idleConnectionTime  |                           int,default is 3000                           |    No    | If the idle time exceeds the set value of this parameter, the connection will be closed. Defaults to 30000milliseconds.      |
|  maxIdleConnections  |                           int,default is 3000                           |    No    | The maximum number of idle connections in the connection pool                                                                |
|    maxConnections    |                           int,default is 1000                           |    No    | Maximum allowed number of concurrent HTTP requests. Default is 1000.                                                         |

### Example
obs-sink-config.yaml
```
configs:
 outDirectory: "/obs_test/parquet"
 parquetWriterVersion: "v2"
 parquetWriterMode: "create"
 fileWriteClass: "org.apache.pulsar.io.sftp.sink.MessageToParquetFileOBSWriter"
 endPoint: https://obs.cn-south-1.myhuaweicloud.com
 accessKey: xxxx
 secretKey: xxxx
 bucket: sn-poc-test
 expires: 1
 numWorkers: 10
```


# Build
1. mvn install
```
mvn clean install -Dmaven.test.skip=true  
```
2. find `pulsar-io-sftp-connector-2.10.1-SNAPSHOT.nar` in target folder

# Overview
This is a sftp io connector source for [pulsar](https://github.com/apache/pulsar).
It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.
This Connector is used to write each file in the specified directory on the sftp server into pulsar in the form of a
message.

# Architecture
![image](https://user-images.githubusercontent.com/33149602/199865567-43776801-f120-4f83-a17e-4d9e0d38e4df.png)

# TaskProgress
 
| Settings       | Output type | Explanation                                |
|----------------|:-----------:|:-------------------------------------------|
| taskId         |   string    | taskId                                     |
| taskType       |   string    | taskType, such as : source,sink            |
| taskCategory   |   string    | taskCategory, such as : task.progress.sftp |
| timestamp      |     int     | timestamp                                  |
| taskProperties |   string    | taskProperties  Map<String, String>        |
| state          |   string    | state, such as : Success, Failed           |

# Pulsar Sftp Source

### Pulsar Sftp Source Configuration Options
| Settings                      |                  Output type                  | Required | Explanation                                                                                          |
|-------------------------------|:---------------------------------------------:|----------|:-----------------------------------------------------------------------------------------------------|
| host                          |                    string                     | Yes      | Sftp server host                                                                                     |
| port                          |               int,default is 22               | No       | Sftp server port                                                                                     |
| username                      |                    string                     | Yes      | Sftp server username                                                                                 |
| password                      |                    string                     | No       | Sftp server password                                                                                 |
| privateKey                    |                    string                     | No       | Sftp server privateKey                                                                               |
| inputDirectory                |                    string                     | Yes      | The input directory from which to pull files.                                                        |
| movedDirectory                |                    string                     | Yes      | The pulled files will be moved to this directory.                                                    |
| illegalFileDirectory          |                    string                     | Yes      | If the file size greater than "maximumSize" and the file is illegal will be moved to this directory. |
| recurse                       | boolean, one of [true, false].default is true | No       | Indicates whether or not to pull files from sub-directories.                                         |
| keepFile                      | boolean, one of [true, false].default is true | No       | The file is not deleted after it has been processed and move to "movedDirectory".                    |
| fileFilter                    |          string,default is "[^.].*"           | No       | Only files whose names match the given regular expression will be picked up.                         |
| maximumSize                   |           Long,default is 1048576L            | No       | The maximum size (in Bytes) that a file can be in order to be processed.                             |
| maxFileNumListing             |              int,default is 100               | No       | The maxi num  file  that a sftp listing can be in order to be processed.                             |
| ignoreHiddenFiles             | boolean, one of [true, false].default is true | No       | Indicates whether or not hidden files should be ignored or not.                                      |
| pollingInterval               |            Long,default is 10000L             | No       | Indicates how long to wait before performing a directory listing.                                    |
| numWorkers                    |               int,default is 1                | No       | The number of worker threads that will be processing the files.                                      |
| sftpTaskTopic                 |         string,default is "sftp_task"         | No       | Synchronization tasks topic name.                                                                    |
| sftpTaskTopicSubscriptionName |       string,default is "sftp_task_sub"       | No       | Synchronization tasks subscript name.                                                                |
| taskProgressTopic             |       string,default is "taskProgress"        | No       | record task progress topic name.                                                                     |

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
| Settings                        |                                                                 Output type                                                                 | Required | Explanation                                                                                          |
|---------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------:|----------|:-----------------------------------------------------------------------------------------------------|
| outDirectory                    |                                                                   string                                                                    | Yes      | Sftp server host                                                                                     |
| fileWriteClass                  | string,one of [org.apache.pulsar.io.sftp.sink.MessageToParquetFileOBSWriter, <br/>org.apache.pulsar.io.sftp.sink.MessageToRAWFileOBSWriter] | Yes      | Sftp server port                                                                                     |
| parquetWriterVersion            |                         string,one of [v1,v2]                                                                string                         | Yes      | Sftp server username                                                                                 |
| parquetWriterMode               |                                                                   string                                                                    | No       | Sftp server password                                                                                 |
| compressionCodecName            |                                                                   string                                                                    | No       | Sftp server privateKey                                                                               |
| enableDictionary                |                                                                   string                                                                    | Yes      | The input directory from which to pull files.                                                        |
| enableValidation                |                                                                   string                                                                    | Yes      | The pulled files will be moved to this directory.                                                    |
| defaultPageSize                 |                                                                   string                                                                    | Yes      | If the file size greater than "maximumSize" and the file is illegal will be moved to this directory. |
| dictionaryPageSize              |                                                boolean, one of [true, false].default is true                                                | No       | Indicates whether or not to pull files from sub-directories.                                         |
| maxPaddingSize                  |                                                boolean, one of [true, false].default is true                                                | No       | The file is not deleted after it has been processed and move to "movedDirectory".                    |
| defaultBlockSize                |                                                         string,default is "[^.].*"                                                          | No       | Only files whose names match the given regular expression will be picked up.                         |
| taskProgressTopic               |                                                          Long,default is 1048576L                                                           | No       | The maximum size (in Bytes) that a file can be in order to be processed.                             |
| endPoint                        |                                                             int,default is 100                                                              | No       | The maxi num  file  that a sftp listing can be in order to be processed.                             |
| accessKey                       |                                                boolean, one of [true, false].default is true                                                | No       | Indicates whether or not hidden files should be ignored or not.                                      |
| secretKey                       |                                                           Long,default is 10000L                                                            | No       | Indicates how long to wait before performing a directory listing.                                    |
| securityToken                   |                                                              int,default is 1                                                               | No       | The number of worker threads that will be processing the files.                                      |
| bucket                          |                                                        string,default is "sftp_task"                                                        | No       | Synchronization tasks topic name.                                                                    |
| expires                         |                                                      string,default is "sftp_task_sub"                                                      | No       | Synchronization tasks subscript name.                                                                |
| connectionTimeout               |                                                      string,default is "taskProgress"                                                       | No       | record task progress topic name.                                                                     |
| socketTimeout                   |                                                              int,default is 1                                                               | No       | The number of worker threads that will be processing the files.                                      |
| idleConnectionTime              |                                                        string,default is "sftp_task"                                                        | No       | Synchronization tasks topic name.                                                                    |
| maxIdleConnections              |                                                      string,default is "sftp_task_sub"                                                      | No       | Synchronization tasks subscript name.                                                                |
| maxConnections                  |                                                      string,default is "taskProgress"                                                       | No       | record task progress topic name.                                                                     |

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

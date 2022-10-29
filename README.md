# pulsar-io-sftp-connector

This is a sftp io connector source for [pulsar](https://github.com/apache/pulsar).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

This Connector is used to write each file in the specified directory on the sftp server into pulsar in the form of a
message.

# Pulsar Sftp Source Configuration Options

This plugin supports these configuration options.

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

# Example
sftp-source-config.yaml
```
configs:
 host: "127.0.0.1"
 username: "root"
 password: "12345678"
 inputDirectory: "/sftpdata/input"
 movedDirectory: "/sftpdata/moved"
 illegalFileDirectory: "/sftpdata/illegal_file"
```

# Build
1. mvn install
```
mvn clean install -Dmaven.test.skip=true  
```
2. find `pulsar-io-sftp-connector-2.10.1-SNAPSHOT.jar` in target folder

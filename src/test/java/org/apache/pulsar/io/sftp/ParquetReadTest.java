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
package org.apache.pulsar.io.sftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import org.apache.pulsar.io.sftp.utils.Constants;


public class ParquetReadTest {

    public static void main(String args[]) throws IOException {
        Configuration configuration = new Configuration();
        String schemaName = Constants.SCHEMA_NAME;
        String filePath = "/Users/fujun/Desktop/file.parquet";

        // set filter
        //ParquetInputFormat.setFilterPredicate(configuration, lt(longColumn("id"), (long)(5)));
        FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);

        // set schema
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Constants.ID));
        builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Constants.MESSAGE));
        builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, Constants.TOPIC));
        builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, Constants.CREATE_TIME));
        MessageType querySchema = builder.named(schemaName);
        System.out.println("******querySchema.toString()********* : " + querySchema.toString());
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

        // set reader, withConf set specific fields (requested projection), withFilter set the filter.
        // if omit withConf, it queries all fields
        ParquetReader.Builder<Group> reader= ParquetReader
                .builder(new GroupReadSupport(), new Path(filePath))
                .withConf(configuration)
                .withFilter(filter);

        // read
        ParquetReader<Group> build=reader.build();
        Group line;
        while((line=build.read())!=null)
            System.out.println(line);

    }
}

/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.dataload;

import com.alibaba.graphscope.dataload.encode.IrDataEncodeMapper;
import com.alibaba.graphscope.dataload.graph.IrWriteGraphMapper;
import com.alibaba.graphscope.dataload.graph.IrWriteGraphPartitioner;
import com.alibaba.graphscope.dataload.graph.IrWriteGraphReducer;
import com.alibaba.maxgraph.dataload.databuild.OfflineBuildOdps;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class IrDataBuild {
    public static final String COLUMN_MAPPING_META = "column.mapping.meta";
    public static final String SPLIT_SIZE = "split.size";
    public static final String SEPARATOR = "separator";
    public static final String UNIQUE_NAME = "unique.name";

    // ENCODE/WRITE_GRAPH
    public static final String DATA_BUILD_MODE = "data.build.mode";
    public static final String ENCODE_OUTPUT_TABLE_NUM = "encode.output.table.num";
    public static final String SKIP_HEADER = "skip.header";

    public static final String GRAPH_REDUCER_NUM = "write.graph.reducer.num";

    public static final String ENCODE_VERTEX_MAGIC = "_encode_vertex_";
    public static final String ENCODE_EDGE_MAGIC = "_encode_edge_";
    public static final int PROP_BUFFER_SIZE = 2048;

    public static void main(String[] args) throws Exception {
        File file = new File(args[0]);
        Properties properties = new Properties();
        properties.load(new FileInputStream(file));

        String mode = properties.getProperty(DATA_BUILD_MODE, "ENCODE");
        int splitSize = Integer.valueOf(properties.getProperty(SPLIT_SIZE, "256"));
        String columnMappingJson = properties.getProperty(COLUMN_MAPPING_META, "");
        String prefix = properties.getProperty(UNIQUE_NAME, "default");
        String tableNum = properties.getProperty(ENCODE_OUTPUT_TABLE_NUM, "1");
        String separator = properties.getProperty(SEPARATOR, "|");
        int reducerNum = Integer.valueOf(properties.getProperty(GRAPH_REDUCER_NUM, "1"));
        String skipHeader = properties.getProperty(SKIP_HEADER, "true");

        // oss config
        String endpoint = properties.getProperty(OfflineBuildOdps.OSS_ENDPOINT);
        String accessId = properties.getProperty(OfflineBuildOdps.OSS_ACCESS_ID);
        String accessKey = properties.getProperty(OfflineBuildOdps.OSS_ACCESS_KEY);
        String bucketName = properties.getProperty(OfflineBuildOdps.OSS_BUCKET_NAME);

        JobConf job = new JobConf();
        if (mode.equals("ENCODE")) {
            job.setSplitSize(splitSize); // split size determines the num of mapper tasks
            job.setMapperClass(IrDataEncodeMapper.class);
            job.setNumReduceTasks(0);
            job.set(COLUMN_MAPPING_META, columnMappingJson);
            job.set(ENCODE_OUTPUT_TABLE_NUM, tableNum);
            job.set(UNIQUE_NAME, prefix);
            job.set(OfflineBuildOdps.OSS_ENDPOINT, endpoint);
            job.set(OfflineBuildOdps.OSS_ACCESS_ID, accessId);
            job.set(OfflineBuildOdps.OSS_ACCESS_KEY, accessKey);
            job.set(OfflineBuildOdps.OSS_BUCKET_NAME, bucketName);
            job.set(SEPARATOR, separator);
            job.setBoolean(SKIP_HEADER, Boolean.valueOf(skipHeader));
            loadEncodeInputTable(job, properties);
            loadEncodeOutputTable(job, properties);
        } else if (mode.equals("WRITE_GRAPH")) {
            job.setSplitSize(splitSize);
            job.setMapperClass(IrWriteGraphMapper.class);
            job.setNumReduceTasks(reducerNum);
            job.setReducerClass(IrWriteGraphReducer.class);
            job.setPartitionerClass(IrWriteGraphPartitioner.class);
            job.set(GRAPH_REDUCER_NUM, String.valueOf(reducerNum));
            job.set(UNIQUE_NAME, prefix);
            job.set(OfflineBuildOdps.OSS_ENDPOINT, endpoint);
            job.set(OfflineBuildOdps.OSS_ACCESS_ID, accessId);
            job.set(OfflineBuildOdps.OSS_ACCESS_KEY, accessKey);
            job.set(OfflineBuildOdps.OSS_BUCKET_NAME, bucketName);
            job.setMapOutputKeySchema(
                    SchemaUtils.fromString(
                            "id:bigint,type:bigint")); // globalId for partition, typeId (vertex or
            // edge)
            job.setMapOutputValueSchema(
                    SchemaUtils.fromString(
                            "id1:bigint,id2:bigint,id3:bigint,id4:bigint,id5:bigint,bytes:string,len:bigint,code:bigint"));
            loadGraphInputTable(job, properties);
        } else {
            throw new IllegalArgumentException(
                    "invalid mode " + mode + ", use ENCODE or WRITE_GRAPH instead");
        }
        JobClient.runJob(job);
    }

    private static void loadEncodeInputTable(JobConf job, Properties properties) throws Exception {
        String columnMappingJson = properties.getProperty(COLUMN_MAPPING_META, "");
        ColumnMappingMeta columnMappingMeta =
                (new ObjectMapper())
                        .readValue(columnMappingJson, new TypeReference<ColumnMappingMeta>() {})
                        .init();
        List<String> tableNames = columnMappingMeta.getTableNames();
        tableNames.forEach(
                t -> {
                    InputUtils.addTable(parseTableURL(t, Optional.empty()), job);
                });
    }

    private static void loadEncodeOutputTable(JobConf job, Properties properties) throws Exception {
        List<String> outputTables = makeEncodeOutputTableNames(properties);
        outputTables.forEach(
                s -> {
                    OutputUtils.addTable(parseTableURL(s, Optional.of(s)), job);
                });
    }

    private static void loadGraphInputTable(JobConf job, Properties properties) throws Exception {
        List<String> inputTables = makeEncodeOutputTableNames(properties);
        inputTables.forEach(
                s -> {
                    InputUtils.addTable(parseTableURL(s, Optional.empty()), job);
                });
    }

    private static List<String> makeEncodeOutputTableNames(Properties properties) {
        int partitions = Integer.valueOf(properties.getProperty(ENCODE_OUTPUT_TABLE_NUM, "1"));
        String prefix = properties.getProperty(UNIQUE_NAME);
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < partitions; ++i) {
            tableNames.add(prefix + ENCODE_VERTEX_MAGIC + i);
            tableNames.add(prefix + ENCODE_EDGE_MAGIC + i);
        }
        return tableNames;
    }

    private static TableInfo parseTableURL(String tableFullName, Optional<String> labelOpt) {
        String projectName = null;
        String tableName = null;
        String partitionSpec = null;
        if (tableFullName.contains(".")) {
            String[] items = tableFullName.split("\\.");
            projectName = items[0];
            tableName = items[1];
        } else {
            tableName = tableFullName;
        }
        if (tableName.contains("|")) {
            String[] items = tableName.split("\\|");
            tableName = items[0];
            partitionSpec = items[1];
        }

        TableInfo.TableInfoBuilder builder = TableInfo.builder();
        if (projectName != null) {
            builder.projectName(projectName);
        }
        builder.tableName(tableName);
        if (partitionSpec != null) {
            builder.partSpec(partitionSpec);
        }
        if (labelOpt.isPresent()) {
            builder.label(labelOpt.get());
        }
        return builder.build();
    }
}

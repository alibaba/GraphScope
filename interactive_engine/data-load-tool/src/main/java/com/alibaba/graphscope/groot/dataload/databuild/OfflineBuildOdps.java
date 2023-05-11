/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.compiler.api.schema.*;
import com.alibaba.graphscope.compiler.api.schema.GraphEdge;
import com.alibaba.graphscope.compiler.api.schema.GraphElement;
import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.groot.dataload.util.Constants;
import com.alibaba.graphscope.groot.dataload.util.OSSFS;
import com.alibaba.graphscope.groot.dataload.util.VolumeFS;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.sdkcommon.common.DataLoadTarget;
import com.alibaba.graphscope.sdkcommon.schema.GraphSchemaMapper;
import com.alibaba.graphscope.sdkcommon.util.UuidUtils;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

public class OfflineBuildOdps {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuildOdps.class);

    private static Odps odps;

    public static void main(String[] args) throws IOException {
        String propertiesFile = args[0];

        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            properties.load(is);
        }
        odps = SessionState.get().getOdps();

        String columnMappingConfigStr = properties.getProperty(Constants.COLUMN_MAPPING_CONFIG);
        String graphEndpoint = properties.getProperty(Constants.GRAPH_ENDPOINT);
        String username = properties.getProperty(Constants.USER_NAME, "");
        String password = properties.getProperty(Constants.PASS_WORD, "");

        String uniquePath =
                properties.getProperty(Constants.UNIQUE_PATH, UuidUtils.getBase64UUIDString());

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, FileColumnMapping> columnMappingConfig =
                objectMapper.readValue(
                        columnMappingConfigStr,
                        new TypeReference<Map<String, FileColumnMapping>>() {});

        List<DataLoadTarget> targets = new ArrayList<>();
        for (FileColumnMapping fileColumnMapping : columnMappingConfig.values()) {
            targets.add(
                    DataLoadTarget.newBuilder()
                            .setLabel(fileColumnMapping.getLabel())
                            .setSrcLabel(fileColumnMapping.getSrcLabel())
                            .setDstLabel(fileColumnMapping.getDstLabel())
                            .build());
        }

        GrootClient client =
                GrootClient.newBuilder()
                        .setHosts(graphEndpoint)
                        .setUsername(username)
                        .setPassword(password)
                        .build();

        GraphSchema schema = client.prepareDataLoad(targets);
        String schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();
        // number of reduce task
        int partitionNum = client.getPartitionNum();

        Map<String, GraphElement> tableType = new HashMap<>();

        Map<String, ColumnMappingInfo> columnMappingInfos = new HashMap<>();
        columnMappingConfig.forEach(
                (fileName, fileColumnMapping) -> {
                    ColumnMappingInfo columnMappingInfo =
                            fileColumnMapping.toColumnMappingInfo(schema);
                    // Note the project and partition is stripped (if exists)
                    columnMappingInfos.put(getTableName(fileName), columnMappingInfo);
                    tableType.put(fileName, schema.getElement(columnMappingInfo.getLabelId()));
                });
        long splitSize = Long.parseLong(properties.getProperty(Constants.SPLIT_SIZE, "256"));

        JobConf job = new JobConf();
        String mappings = objectMapper.writeValueAsString(columnMappingInfos);

        // Avoid java sandbox protection
        job.set("odps.isolation.session.enable", "true");
        // Don't introduce legacy jar files
        job.set("odps.sql.udf.java.retain.legacy", "false");
        // Default priority is 9
        job.setInstancePriority(0);
        job.set("odps.mr.run.mode", "sql");
        job.set("odps.mr.sql.group.enable", "true");
        job.setFunctionTimeout(2400);
        job.setMemoryForReducerJVM(2048);

        for (Map.Entry<String, GraphElement> entry : tableType.entrySet()) {
            if (entry.getValue() instanceof GraphVertex || entry.getValue() instanceof GraphEdge) {
                String name = entry.getKey();
                InputUtils.addTable(parseTableURL(name), job);
            }
        }

        job.setMapperClass(DataBuildMapperOdps.class);
        job.setReducerClass(DataBuildReducerOdps.class);
        job.setPartitionerClass(DataBuildPartitionerOdps.class);
        job.setSplitSize(splitSize);
        job.setNumReduceTasks(partitionNum);
        job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));

        String dataSinkType = properties.getProperty(Constants.DATA_SINK_TYPE, "VOLUME");
        Map<String, String> config;
        String fullQualifiedDataPath;
        if (dataSinkType.equalsIgnoreCase("VOLUME")) {
            try (VolumeFS fs = new VolumeFS(properties)) {
                fs.setJobConf(job);
                config = fs.setConfig(odps);
                fullQualifiedDataPath = fs.getQualifiedPath();
                fs.createVolumeIfNotExists(odps);
                OutputUtils.addVolume(fs.getVolumeInfo(), job);
            }
        } else if (dataSinkType.equalsIgnoreCase("OSS")) {
            try (OSSFS fs = new OSSFS(properties)) {
                fs.setJobConf(job);
                config = fs.getConfig();
                fullQualifiedDataPath = fs.getQualifiedPath();
            }
            String outputTable = properties.getProperty(Constants.OUTPUT_TABLE);
            OutputUtils.addTable(parseTableURL(outputTable), job);
        } else if (dataSinkType.equalsIgnoreCase("HDFS")) {
            throw new IOException("HDFS as a data sink is not supported in ODPS");
        } else {
            throw new IOException("Unsupported data sink: " + dataSinkType);
        }
        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put(Constants.GRAPH_ENDPOINT, graphEndpoint);
        outputMeta.put(Constants.SCHEMA_JSON, schemaJson);
        outputMeta.put(Constants.COLUMN_MAPPINGS, mappings);
        outputMeta.put(Constants.UNIQUE_PATH, uniquePath);
        outputMeta.put(Constants.DATA_SINK_TYPE, dataSinkType);

        job.set(Constants.META_INFO, objectMapper.writeValueAsString(outputMeta));
        job.set(Constants.DATA_SINK_TYPE, dataSinkType);
        try {
            JobClient.runJob(job);
        } catch (Exception e) {
            throw new IOException(e);
        }

        boolean loadAfterBuild =
                properties
                        .getProperty(Constants.LOAD_AFTER_BUILD, "false")
                        .equalsIgnoreCase("true");
        if (loadAfterBuild) {
            fullQualifiedDataPath = fullQualifiedDataPath + uniquePath;
            logger.info("start ingesting data from " + fullQualifiedDataPath);
            client.ingestData(fullQualifiedDataPath, config);

            logger.info("start committing bulk load");
            Map<Long, DataLoadTarget> tableToTarget = new HashMap<>();
            for (ColumnMappingInfo columnMappingInfo : columnMappingInfos.values()) {
                long tableId = columnMappingInfo.getTableId();
                int labelId = columnMappingInfo.getLabelId();
                GraphElement graphElement = schema.getElement(labelId);
                String label = graphElement.getLabel();
                DataLoadTarget.Builder builder = DataLoadTarget.newBuilder();
                builder.setLabel(label);
                if (graphElement instanceof GraphEdge) {
                    builder.setSrcLabel(
                            schema.getElement(columnMappingInfo.getSrcLabelId()).getLabel());
                    builder.setDstLabel(
                            schema.getElement(columnMappingInfo.getDstLabelId()).getLabel());
                }
                tableToTarget.put(tableId, builder.build());
            }
            client.commitDataLoad(tableToTarget, uniquePath);
        }
        client.close();
    }

    private static String getTableName(String tableFullName) {
        TableInfo info = parseTableURL(tableFullName);
        return info.getTableName();
    }

    /**
     * Parse table URL to @TableInfo
     * @param url the pattern of [projectName.]tableName[|partitionSpec]
     * @return TableInfo
     */
    private static TableInfo parseTableURL(String url) {
        String projectName = odps.getDefaultProject();
        String tableName;
        String partitionSpec = null;
        if (url.contains(".")) {
            String[] items = url.split("\\.");
            projectName = items[0];
            tableName = items[1];
        } else {
            tableName = url;
        }
        if (tableName.contains("|")) {
            String[] items = tableName.split("\\|");
            tableName = items[0];
            partitionSpec = items[1];
        }

        TableInfo.TableInfoBuilder builder = TableInfo.builder();
        builder.projectName(projectName);

        builder.tableName(tableName);
        if (partitionSpec != null) {
            builder.partSpec(partitionSpec);
        }
        return builder.build();
    }
}

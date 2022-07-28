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
package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.graphscope.groot.sdk.MaxGraphClient;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.sdkcommon.common.DataLoadTarget;
import com.alibaba.maxgraph.sdkcommon.schema.GraphSchemaMapper;
import com.alibaba.maxgraph.sdkcommon.util.UuidUtils;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
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

    public static final String GRAPH_ENDPOINT = "graph.endpoint";
    public static final String SCHEMA_JSON = "schema.json";
    public static final String SEPARATOR = "separator";
    public static final String COLUMN_MAPPING_CONFIG = "column.mapping.config";
    public static final String SPLIT_SIZE = "split.size";

    public static final String COLUMN_MAPPINGS = "column.mappings";
    public static final String LDBC_CUSTOMIZE = "ldbc.customize";
    public static final String LOAD_AFTER_BUILD = "load.after.build";
    public static final String SKIP_HEADER = "skip.header";

    public static final String OUTPUT_TABLE = "output.table";
    public static final String OSS_ACCESS_ID = "oss.access.id";
    public static final String OSS_ACCESS_KEY = "oss.access.key";
    public static final String OSS_ENDPOINT = "oss.endpoint";
    public static final String OSS_BUCKET_NAME = "oss.bucket.name";
    public static final String OSS_OBJECT_NAME = "oss.object.name";
    public static final String META_INFO = "meta.info";
    public static final String USER_NAME = "auth.username";
    public static final String PASS_WORD = "auth.password";
    public static final String UNIQUE_PATH = "unique.path";

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        String propertiesFile = args[0];
        String uniquePath = UuidUtils.getBase64UUIDString();
        // User can assign a unique path manually.
        if (args.length > 1) {
            uniquePath = args[1];
        }

        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            properties.load(is);
        }

        String outputTable = properties.getProperty(OUTPUT_TABLE);
        String ossAccessId = properties.getProperty(OSS_ACCESS_ID);
        String ossAccessKey = properties.getProperty(OSS_ACCESS_KEY);
        String ossEndPoint = properties.getProperty(OSS_ENDPOINT);
        String ossBucketName = properties.getProperty(OSS_BUCKET_NAME);
        String ossObjectName = properties.getProperty(OSS_OBJECT_NAME);

        String columnMappingConfigStr = properties.getProperty(COLUMN_MAPPING_CONFIG);
        String graphEndpoint = properties.getProperty(GRAPH_ENDPOINT);
        String username = properties.getProperty(USER_NAME);
        String password = properties.getProperty(PASS_WORD);

        MaxGraphClient client =
                MaxGraphClient.newBuilder()
                        .setHosts(graphEndpoint)
                        .setUsername(username)
                        .setPassword(password)
                        .build();
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
        GraphSchema schema = client.prepareDataLoad(targets);
        String schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();
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
        String ldbcCustomize = properties.getProperty(LDBC_CUSTOMIZE, "true");
        long splitSize = Long.valueOf(properties.getProperty(SPLIT_SIZE, "256"));
        boolean skipHeader = properties.getProperty(SKIP_HEADER, "true").equalsIgnoreCase("true");

        JobConf job = new JobConf();
        job.set(SCHEMA_JSON, schemaJson);
        String mappings = objectMapper.writeValueAsString(columnMappingInfos);
        job.set(COLUMN_MAPPINGS, mappings);
        job.setBoolean(LDBC_CUSTOMIZE, ldbcCustomize.equalsIgnoreCase("true"));
        job.set(SEPARATOR, properties.getProperty(SEPARATOR, "\\|"));
        job.setBoolean(SKIP_HEADER, skipHeader);
        job.set(GRAPH_ENDPOINT, graphEndpoint);

        job.set(OSS_ACCESS_ID, ossAccessId);
        job.set(OSS_ACCESS_KEY, ossAccessKey);
        job.set(OSS_ENDPOINT, ossEndPoint);
        job.set(OSS_BUCKET_NAME, ossBucketName);
        job.set(OSS_OBJECT_NAME, ossObjectName);

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

        OutputUtils.addTable(parseTableURL(outputTable), job);

        String dataPath = Paths.get(ossBucketName, ossObjectName).toString();
        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put("endpoint", graphEndpoint);
        outputMeta.put("schema", schemaJson);
        outputMeta.put("mappings", mappings);
        outputMeta.put("datapath", dataPath);
        outputMeta.put("unique_path", uniquePath);

        job.set(META_INFO, objectMapper.writeValueAsString(outputMeta));
        job.set(UNIQUE_PATH, uniquePath);

        System.out.println("uniquePath is: " + uniquePath);

        try {
            JobClient.runJob(job);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static String getTableName(String tableFullName) {
        String tableName;
        if (tableFullName.contains(".")) {
            String[] items = tableFullName.split("\\.");
            tableName = items[1];
        } else {
            tableName = tableFullName;
        }
        if (tableName.contains("|")) {
            String[] items = tableName.split("\\|");
            tableName = items[0];
        }
        return tableName;
    }

    private static TableInfo parseTableURL(String tableFullName) {
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
        return builder.build();
    }
}

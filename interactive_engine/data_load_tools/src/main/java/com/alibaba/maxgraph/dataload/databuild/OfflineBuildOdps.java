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


import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.graphscope.groot.schema.*;
import com.alibaba.graphscope.groot.sdk.Client;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.sdkcommon.common.DataLoadTarget;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import java.io.IOException;
import java.util.Iterator;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class OfflineBuildOdps {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuildOdps.class);

    public static final String INPUT_PATH = "input.path";
    public static final String OUTPUT_PATH = "output.path";
    public static final String GRAPH_ENDPOINT = "graph.endpoint";
    public static final String SCHEMA_JSON = "schema.json";
    public static final String SEPARATOR = "separator";
    public static final String COLUMN_MAPPING_CONFIG = "column.mapping.config";
    public static final String SPLIT_SIZE = "split.size";

    public static final String COLUMN_MAPPINGS = "column.mappings";
    public static final String LDBC_CUSTOMIZE = "ldbc.customize";
    public static final String LOAD_AFTER_BUILD = "load.after.build";
    public static final String SKIP_HEADER = "skip.header";
    public static final String PARTITION_NUM = "partition.num";

    public static final String PROJECT = "project";
    public static final String OUTPUT_TABLE = "output.table";
    public static final String OSS_ACCESS_ID = "oss.access.id";
    public static final String OSS_ACCESS_KEY = "oss.access.key";
    public static final String OSS_ENDPOINT = "oss.endpoint";
    public static final String OSS_BUCKET_NAME = "oss.bucket.name";
    public static final String OSS_OBJECT_NAME = "oss.object.name";

    public static final String VERTEX_TABLE_NAME = "vertexTableName";
    public static final String EDGE_TABLE_NAME = "edgeTableName";

    public static LinkedHashMap<String, String> convertPartSpecToMap(
        String partSpec) {
      LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
      if (partSpec != null && !partSpec.trim().isEmpty()) {
        String[] parts = partSpec.split("/");
        for (String part : parts) {
          String[] ss = part.split("=");
          if (ss.length != 2) {
            throw new RuntimeException("ODPS-0730001: error part spec format: "
                + partSpec);
          }
          map.put(ss[0], ss[1]);
        }
      }
      return map;
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        String propertiesFile = args[0];
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            properties.load(is);
        }

        String project = properties.getProperty(PROJECT);
        String outputTable = properties.getProperty(OUTPUT_TABLE);
        String ossAccessId = properties.getProperty(OSS_ACCESS_ID);
        String ossAccessKey = properties.getProperty(OSS_ACCESS_KEY);
        String ossEndPoint = properties.getProperty(OSS_ENDPOINT);
        String ossBucketName = properties.getProperty(OSS_BUCKET_NAME);
        String ossObjectName = properties.getProperty(OSS_OBJECT_NAME);
        String partitionNumConf = properties.getProperty(PARTITION_NUM);

        String inputPath = properties.getProperty(INPUT_PATH);
        String outputPath = properties.getProperty(OUTPUT_PATH);
        String columnMappingConfigStr = properties.getProperty(COLUMN_MAPPING_CONFIG);
        String graphEndpoint = properties.getProperty(GRAPH_ENDPOINT);
        Client client = new Client(graphEndpoint);
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
        if (partitionNumConf != null && !partitionNumConf.equals("")) {
            partitionNum = Integer.valueOf(partitionNumConf).intValue();
        }

        Map<String, GraphElement> tableType = new HashMap<>();

        Map<String, ColumnMappingInfo> columnMappingInfos = new HashMap<>();
        columnMappingConfig.forEach(
                (fileName, fileColumnMapping) -> {
                    ColumnMappingInfo columnMappingInfo = fileColumnMapping.toColumnMappingInfo(schema);
                    columnMappingInfos.put(fileName, columnMappingInfo);
                    tableType.put(fileName, schema.getElement(columnMappingInfo.getLabelId()));
                });
        String ldbcCustomize = properties.getProperty(LDBC_CUSTOMIZE, "true");
        long splitSize = Long.valueOf(properties.getProperty(SPLIT_SIZE, "256")) * 1024 * 1024;
        boolean loadAfterBuild =
                properties.getProperty(LOAD_AFTER_BUILD, "false").equalsIgnoreCase("true");
        boolean skipHeader = properties.getProperty(SKIP_HEADER, "true").equalsIgnoreCase("true");


        JobConf job = new JobConf();
        job.set(SCHEMA_JSON, schemaJson);
        String mappings = objectMapper.writeValueAsString(columnMappingInfos);
        job.set(COLUMN_MAPPINGS, mappings);
        job.setBoolean(LDBC_CUSTOMIZE, ldbcCustomize.equalsIgnoreCase("true"));
        job.set(SEPARATOR, properties.getProperty(SEPARATOR, "\\|"));
        job.setBoolean(SKIP_HEADER, skipHeader);

        job.set(OSS_ACCESS_ID, ossAccessId);
        job.set(OSS_ACCESS_KEY, ossAccessKey);
        job.set(OSS_ENDPOINT, ossEndPoint);
        job.set(OSS_BUCKET_NAME, ossBucketName);
        job.set(OSS_OBJECT_NAME, ossObjectName);

        for (Map.Entry<String, GraphElement> entry : tableType.entrySet()) {
            if (entry.getValue() instanceof GraphVertex) {
                job.set(VERTEX_TABLE_NAME, entry.getKey());
                InputUtils.addTable(TableInfo.builder().tableName(entry.getKey()).build(), job);
            }
            if (entry.getValue() instanceof GraphEdge) {
                job.set(EDGE_TABLE_NAME, entry.getKey());
                InputUtils.addTable(TableInfo.builder().tableName(entry.getKey()).build(), job);
            }
        }

        job.setMapperClass(DataBuildMapperOdps.class);
        job.setReducerClass(DataBuildReducerOdps.class);
        job.setNumReduceTasks(partitionNum);
        job.setMapOutputKeySchema(SchemaUtils.fromString("type:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("content:string"));

        OutputUtils.addTable(TableInfo.builder().tableName(outputTable).build(), job);
        try {
            JobClient.runJob(job);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

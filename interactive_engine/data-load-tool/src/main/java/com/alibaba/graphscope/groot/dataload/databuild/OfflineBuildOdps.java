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

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.mapper.GraphSchemaMapper;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.util.JSON;
import com.alibaba.graphscope.groot.common.util.UuidUtils;
import com.alibaba.graphscope.groot.dataload.util.OSSFS;
import com.alibaba.graphscope.groot.dataload.util.VolumeFS;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.proto.groot.DataLoadTargetPb;
import com.alibaba.graphscope.proto.groot.GraphDefPb;
import com.aliyun.odps.Odps;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

        String configStr = properties.getProperty(DataLoadConfig.COLUMN_MAPPING_CONFIG);
        configStr = Utils.replaceVars(configStr, Arrays.copyOfRange(args, 1, args.length));

        String graphEndpoint = properties.getProperty(DataLoadConfig.GRAPH_ENDPOINT);
        String username = properties.getProperty(DataLoadConfig.USER_NAME, "");
        String password = properties.getProperty(DataLoadConfig.PASS_WORD, "");

        String uniquePath =
                properties.getProperty(DataLoadConfig.UNIQUE_PATH, UuidUtils.getBase64UUIDString());

        Map<String, FileColumnMapping> mappingConfig = Utils.parseColumnMapping(configStr);

        List<DataLoadTargetPb> targets = Utils.getDataLoadTargets(mappingConfig);

        GrootClient client = Utils.getClient(graphEndpoint, username, password);

        GraphDefPb graphDefPb = client.prepareDataLoad(targets);
        System.out.println("GraphDef: " + graphDefPb);
        GraphSchema schema = GraphDef.parseProto(graphDefPb);
        System.out.println("GraphSchema: " + JSON.toJson(schema));

        // number of reduce task
        int partitionNum = client.getPartitionNum();
        long splitSize = Long.parseLong(properties.getProperty(DataLoadConfig.SPLIT_SIZE, "256"));

        JobConf job = new JobConf();

        job.set("odps.isolation.session.enable", "true"); // Avoid java sandbox protection
        job.set("odps.sql.udf.java.retain.legacy", "false"); // exclude legacy jar files
        job.setInstancePriority(0); // Default priority is 9
        // Disable backups from competing with original instances
        job.set("odps.sql.backupinstance.enabled", "false");
        job.setFunctionTimeout(2400);
        job.setMemoryForReducerJVM(4096);

        job.setMapperClass(DataBuildMapperOdps.class);
        job.setReducerClass(DataBuildReducerOdps.class);
        job.setPartitionerClass(DataBuildPartitionerOdps.class);
        job.setSplitSize(splitSize);
        job.setNumReduceTasks(partitionNum);
        job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));

        mappingConfig.forEach(
                (name, x) -> InputUtils.addTable(Utils.parseTableURL(odps, name), job));

        String dataSinkType = properties.getProperty(DataLoadConfig.DATA_SINK_TYPE, "VOLUME");
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
            String outputTable = properties.getProperty(DataLoadConfig.OUTPUT_TABLE);
            OutputUtils.addTable(Utils.parseTableURL(odps, outputTable), job);
        } else if (dataSinkType.equalsIgnoreCase("HDFS")) {
            throw new IOException("HDFS as a data sink is not supported in ODPS");
        } else {
            throw new IOException("Unsupported data sink: " + dataSinkType);
        }

        String schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();
        System.out.println("schemaJson is :" + schemaJson);
        Map<String, ColumnMappingInfo> info = Utils.getMappingInfo(odps, schema, mappingConfig);
        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put(DataLoadConfig.GRAPH_ENDPOINT, graphEndpoint);
        outputMeta.put(DataLoadConfig.SCHEMA_JSON, schemaJson);
        outputMeta.put(DataLoadConfig.COLUMN_MAPPINGS, mapper.writeValueAsString(info));
        outputMeta.put(DataLoadConfig.UNIQUE_PATH, uniquePath);
        outputMeta.put(DataLoadConfig.DATA_SINK_TYPE, dataSinkType);

        job.set(DataLoadConfig.META_INFO, mapper.writeValueAsString(outputMeta));
        job.set(DataLoadConfig.DATA_SINK_TYPE, dataSinkType);
        try {
            JobClient.runJob(job);
        } catch (Exception e) {
            throw new IOException(e);
        }

        String _tmp = properties.getProperty(DataLoadConfig.LOAD_AFTER_BUILD, "false");
        boolean loadAfterBuild = Utils.parseBoolean(_tmp);
        if (loadAfterBuild) {
            fullQualifiedDataPath = fullQualifiedDataPath + uniquePath;
            logger.info("start ingesting data from " + fullQualifiedDataPath);
            logger.info("Ingesting data with config:");
            config.forEach((key, value) -> logger.info(key + "=" + value));
            try {
                client.ingestData(fullQualifiedDataPath, config);
                logger.info("start committing bulk load");
                Map<Long, DataLoadTargetPb> tableToTarget = Utils.getTableToTargets(schema, info);
                client.commitDataLoad(tableToTarget, uniquePath);
            } finally {
                try {
                    client.clearIngest(uniquePath);
                } catch (Exception e) {
                    logger.warn("Clear ingest failed, ignored");
                }
            }
        }
    }
}

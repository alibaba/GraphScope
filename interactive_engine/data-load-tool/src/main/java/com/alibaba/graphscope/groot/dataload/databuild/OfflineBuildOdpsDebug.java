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
import com.alibaba.graphscope.groot.dataload.unified.UniConfig;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.proto.groot.DataLoadTargetPb;
import com.alibaba.graphscope.proto.groot.GraphDefPb;
import com.aliyun.odps.Odps;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OfflineBuildOdpsDebug {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuildOdpsDebug.class);

    private static Odps odps;

    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        UniConfig properties = UniConfig.fromFile(configFile);

        odps = SessionState.get().getOdps();

        String configStr = properties.getProperty(DataLoadConfig.COLUMN_MAPPING_CONFIG);
        Map<String, FileColumnMapping> mappingConfig;
        if (configStr == null) {
            mappingConfig = Utils.parseColumnMappingFromUniConfig(properties);
        } else {
            configStr = Utils.replaceVars(configStr, Arrays.copyOfRange(args, 1, args.length));
            mappingConfig = Utils.parseColumnMapping(configStr);
        }

        String graphEndpoint = properties.getProperty(DataLoadConfig.GRAPH_ENDPOINT);
        String username = properties.getProperty(DataLoadConfig.USER_NAME, "");
        String password = properties.getProperty(DataLoadConfig.PASS_WORD, "");

        List<DataLoadTargetPb> targets = Utils.getDataLoadTargets(mappingConfig);

        GrootClient client = Utils.getClient(graphEndpoint, username, password);

        GraphDefPb graphDefPb = client.prepareDataLoad(targets);
        System.out.println("GraphDef: " + graphDefPb);

        JobConf job = new JobConf();

        job.set("odps.isolation.session.enable", "true"); // Avoid java sandbox protection
        job.set("odps.sql.udf.java.retain.legacy", "false"); // exclude legacy jar files
        job.setInstancePriority(0); // Default priority is 9
        job.set("odps.mr.run.mode", "sql");
        job.set("odps.mr.sql.group.enable", "true");

        job.setMapperClass(DataBuildMapperOdpsDebug.class);
        job.setNumReduceTasks(0);

        mappingConfig.forEach(
                (name, x) -> InputUtils.addTable(Utils.parseTableURL(odps, name), job));
        String outputTable = properties.getProperty(DataLoadConfig.OUTPUT_TABLE);
        OutputUtils.addTable(Utils.parseTableURL(odps, outputTable), job);
        GraphSchema schema = GraphDef.parseProto(graphDefPb);

        String schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();
        Map<String, ColumnMappingInfo> info = Utils.getMappingInfo(odps, schema, mappingConfig);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put(DataLoadConfig.SCHEMA_JSON, schemaJson);
        outputMeta.put(DataLoadConfig.COLUMN_MAPPINGS, mapper.writeValueAsString(info));
        job.set(DataLoadConfig.META_INFO, mapper.writeValueAsString(outputMeta));
        System.out.println(mapper.writeValueAsString(outputMeta));
        JobClient.runJob(job);
    }
}

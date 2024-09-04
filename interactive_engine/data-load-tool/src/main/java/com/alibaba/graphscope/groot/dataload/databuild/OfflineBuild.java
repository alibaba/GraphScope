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
import com.alibaba.graphscope.groot.common.util.UuidUtils;
import com.alibaba.graphscope.groot.dataload.unified.UniConfig;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.proto.groot.DataLoadTargetPb;
import com.alibaba.graphscope.proto.groot.GraphDefPb;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class OfflineBuild {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuild.class);

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        String configFile = args[0];
        UniConfig properties = UniConfig.fromFile(configFile);

        String inputPath = properties.getProperty(DataLoadConfig.INPUT_PATH);
        String outputPath = properties.getProperty(DataLoadConfig.OUTPUT_PATH);
        String graphEndpoint = properties.getProperty(DataLoadConfig.GRAPH_ENDPOINT);
        String uniquePath =
                properties.getProperty(DataLoadConfig.UNIQUE_PATH, UuidUtils.getBase64UUIDString());

        String _tmp = properties.getProperty(DataLoadConfig.SPLIT_SIZE, "256");
        long splitSize = Long.parseLong(_tmp) * 1024 * 1024;
        _tmp = properties.getProperty(DataLoadConfig.LOAD_AFTER_BUILD, "false");
        boolean loadAfterBuild = Utils.parseBoolean(_tmp);
        _tmp = properties.getProperty(DataLoadConfig.SKIP_HEADER, "true");
        boolean skipHeader = Utils.parseBoolean(_tmp);
        String separator = properties.getProperty(DataLoadConfig.SEPARATOR, "\\|");

        String username = properties.getProperty(DataLoadConfig.USER_NAME, "");
        String password = properties.getProperty(DataLoadConfig.PASS_WORD, "");

        GrootClient client = Utils.getClient(graphEndpoint, username, password);

        String configStr = properties.getProperty(DataLoadConfig.COLUMN_MAPPING_CONFIG);
        Map<String, FileColumnMapping> mappingConfig;
        if (configStr == null) {
            mappingConfig = Utils.parseColumnMappingFromUniConfig(properties);
        } else {
            mappingConfig = Utils.parseColumnMapping(configStr);
        }
        List<DataLoadTargetPb> targets = Utils.getDataLoadTargets(mappingConfig);
        GraphDefPb pb = client.prepareDataLoad(targets);
        GraphSchema schema = GraphDef.parseProto(pb);
        System.out.println("GraphSchema " + pb);
        int partitionNum = client.getPartitionNum();

        Map<String, ColumnMappingInfo> info = new HashMap<>();
        mappingConfig.forEach(
                (fileName, fileColumnMapping) -> {
                    info.put(fileName, fileColumnMapping.toColumnMappingInfo(schema));
                });
        ObjectMapper mapper = new ObjectMapper();
        String schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERNODE, splitSize);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERRACK, splitSize);
        conf.setStrings(DataLoadConfig.SCHEMA_JSON, schemaJson);
        String mappings = mapper.writeValueAsString(info);
        conf.setStrings(DataLoadConfig.COLUMN_MAPPINGS, mappings);
        conf.set(DataLoadConfig.SEPARATOR, separator);
        conf.setBoolean(DataLoadConfig.SKIP_HEADER, skipHeader);
        Job job = Job.getInstance(conf, "build graph data");
        job.setJarByClass(OfflineBuild.class);
        job.setMapperClass(DataBuildMapper.class);
        job.setPartitionerClass(DataBuildPartitioner.class);
        job.setReducerClass(DataBuildReducer.class);
        job.setNumReduceTasks(partitionNum);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, splitSize);
        LazyOutputFormat.setOutputFormatClass(job, SstOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);

        Path outputDir = new Path(outputPath, uniquePath);
        FileOutputFormat.setOutputPath(job, outputDir);
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put(DataLoadConfig.GRAPH_ENDPOINT, graphEndpoint);
        outputMeta.put(DataLoadConfig.SCHEMA_JSON, schemaJson);
        outputMeta.put(DataLoadConfig.COLUMN_MAPPINGS, mappings);
        outputMeta.put(DataLoadConfig.UNIQUE_PATH, uniquePath);

        FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
        FSDataOutputStream os = fs.create(new Path(outputDir, "META"));
        os.writeUTF(mapper.writeValueAsString(outputMeta));
        os.flush();
        os.close();

        if (loadAfterBuild) {
            String dataPath = fs.makeQualified(outputDir).toString();
            logger.info("start ingesting data from " + dataPath);
            try {
                client.ingestData(dataPath);
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

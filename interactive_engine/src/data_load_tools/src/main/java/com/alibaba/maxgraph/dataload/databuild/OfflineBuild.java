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

import com.alibaba.graphscope.groot.sdk.Client;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.groot.schema.GraphSchemaMapper;
import com.alibaba.maxgraph.sdkcommon.common.DataLoadTarget;
import com.fasterxml.jackson.core.type.TypeReference;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class OfflineBuild {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuild.class);

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

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        String propertiesFile = args[0];
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            properties.load(is);
        }
        String inputPath = properties.getProperty(INPUT_PATH);
        String outputPath = properties.getProperty(OUTPUT_PATH);
        String columnMappingConfigStr = properties.getProperty(COLUMN_MAPPING_CONFIG);
        String graphEndpoint = properties.getProperty(GRAPH_ENDPOINT);
        Client client = new Client(graphEndpoint, "");
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

        Map<String, ColumnMappingInfo> columnMappingInfos = new HashMap<>();
        columnMappingConfig.forEach(
                (fileName, fileColumnMapping) -> {
                    columnMappingInfos.put(fileName, fileColumnMapping.toColumnMappingInfo(schema));
                });
        String ldbcCustomize = properties.getProperty(LDBC_CUSTOMIZE, "true");
        long splitSize = Long.valueOf(properties.getProperty(SPLIT_SIZE, "256")) * 1024 * 1024;
        boolean loadAfterBuild =
                properties.getProperty(LOAD_AFTER_BUILD, "false").equalsIgnoreCase("true");
        boolean skipHeader = properties.getProperty(SKIP_HEADER, "true").equalsIgnoreCase("true");
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERNODE, splitSize);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERRACK, splitSize);
        conf.setStrings(SCHEMA_JSON, schemaJson);
        String mappings = objectMapper.writeValueAsString(columnMappingInfos);
        conf.setStrings(COLUMN_MAPPINGS, mappings);
        conf.setBoolean(LDBC_CUSTOMIZE, ldbcCustomize.equalsIgnoreCase("true"));
        conf.set(SEPARATOR, properties.getProperty(SEPARATOR, "\\|"));
        conf.setBoolean(SKIP_HEADER, skipHeader);
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
        Path outputDir = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
        FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
        String dataPath = fs.makeQualified(outputDir).toString();

        Map<String, String> outputMeta = new HashMap<>();
        outputMeta.put("endpoint", graphEndpoint);
        outputMeta.put("schema", schemaJson);
        outputMeta.put("mappings", mappings);
        outputMeta.put("datapath", dataPath);

        FSDataOutputStream os = fs.create(new Path(outputDir, "META"));
        os.writeUTF(objectMapper.writeValueAsString(outputMeta));
        os.flush();
        os.close();

        if (loadAfterBuild) {
            logger.info("start ingesting data");
            client.ingestData(dataPath);

            logger.info("commit bulk load");
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
            client.commitDataLoad(tableToTarget);
        }
    }
}

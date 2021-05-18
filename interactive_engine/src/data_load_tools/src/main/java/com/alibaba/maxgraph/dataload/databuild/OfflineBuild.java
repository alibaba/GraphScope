package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.schema.GraphSchemaMapper;
import com.alibaba.maxgraph.v2.sdk.Client;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class OfflineBuild {
    private static final Logger logger = LoggerFactory.getLogger(OfflineBuild.class);

    public static final String PARTITION_NUM = "partition.num";
    public static final String INPUT_PATH = "input.path";
    public static final String OUTPUT_PATH = "output.path";
    public static final String SCHEMA_FILE = "schema.file";
    public static final String GRAPH_URL = "graph.url";
    public static final String SCHEMA_JSON = "schema.json";
    public static final String SEPARATOR = "separator";
    public static final String COLUMN_MAPPING_CONFIG = "column.mapping.config";
    public static final String SPLIT_SIZE = "split.size";

    public static final String COLUMN_MAPPINGS = "column.mappings";
    public static final String LDBC_CUSTOMIZE = "ldbc.customize";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String propertiesFile = args[0];
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            properties.load(is);
        }
        int partitionNum = Integer.parseInt(properties.getProperty(PARTITION_NUM));
        String inputPath = properties.getProperty(INPUT_PATH);
        String outputPath = properties.getProperty(OUTPUT_PATH);
        String schemaFile = properties.getProperty(SCHEMA_FILE, "");
        String columnMappingConfigStr = properties.getProperty(COLUMN_MAPPING_CONFIG);
        String schemaJson;
        GraphSchema schema;
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, FileColumnMapping> columnMappingConfig = objectMapper.readValue(columnMappingConfigStr,
                new TypeReference<Map<String, FileColumnMapping>>() {});

        if (schemaFile.isEmpty()) {
            String graphUrl = properties.getProperty(GRAPH_URL);
            Client client = new Client(graphUrl, "");
            List<DataLoadTarget> targets = new ArrayList<>();
            for (FileColumnMapping fileColumnMapping : columnMappingConfig.values()) {
                targets.add(DataLoadTarget.newBuilder()
                        .setLabel(fileColumnMapping.getLabel())
                        .setSrcLabel(fileColumnMapping.getSrcLabel())
                        .setDstLabel(fileColumnMapping.getDstLabel())
                        .build());
            }
            schema = client.prepareDataLoad(targets);
            schemaJson = GraphSchemaMapper.parseFromSchema(schema).toJsonString();
        } else {
            schemaJson = new String(Files.readAllBytes(Paths.get(schemaFile)), StandardCharsets.UTF_8);
            schema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        }

        Map<String, ColumnMappingInfo> columnMappingInfos = new HashMap<>();
        columnMappingConfig.forEach((fileName, fileColumnMapping) -> {
            columnMappingInfos.put(fileName, fileColumnMapping.toColumnMappingInfo(schema));
        });
        String ldbcCustomize = properties.getProperty(LDBC_CUSTOMIZE, "true");
        long splitSize = Long.valueOf(properties.getProperty(SPLIT_SIZE, "256")) * 1024 * 1024;
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERNODE, splitSize);
        conf.setLong(CombineTextInputFormat.SPLIT_MINSIZE_PERRACK, splitSize);
        conf.setStrings(SCHEMA_JSON, schemaJson);
        String mappings = objectMapper.writeValueAsString(columnMappingInfos);
        conf.setStrings(COLUMN_MAPPINGS, mappings);
        conf.setBoolean(LDBC_CUSTOMIZE, ldbcCustomize.equalsIgnoreCase("true"));
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
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        int status = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(mappings);
        System.exit(status);
    }

}

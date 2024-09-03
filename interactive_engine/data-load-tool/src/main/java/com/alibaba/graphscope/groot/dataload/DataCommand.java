package com.alibaba.graphscope.groot.dataload;

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.mapper.GraphSchemaMapper;
import com.alibaba.graphscope.groot.dataload.databuild.ColumnMappingInfo;
import com.alibaba.graphscope.groot.dataload.unified.UniConfig;
import com.alibaba.graphscope.groot.dataload.util.OSSFS;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public abstract class DataCommand {

    private final String configPath;
    protected String dataRootPath;
    protected String graphEndpoint;
    protected GraphSchema schema;
    protected Map<String, ColumnMappingInfo> columnMappingInfos;
    protected String metaData;
    protected String username;
    protected String password;
    protected String uniquePath;

    protected Map<String, String> ingestConfig;

    public DataCommand(String configPath) throws IOException {
        this.configPath = configPath;
        initialize();
    }

    private void initialize() throws IOException {
        UniConfig properties = UniConfig.fromFile(configPath);
        username = properties.getProperty(DataLoadConfig.USER_NAME);
        password = properties.getProperty(DataLoadConfig.PASS_WORD);
        graphEndpoint = properties.getProperty(DataLoadConfig.GRAPH_ENDPOINT);
        String outputPath = properties.getProperty(DataLoadConfig.OUTPUT_PATH);
        String metaFilePath = new Path(outputPath, DataLoadConfig.META_FILE_NAME).toString();
        String dataSinkType = properties.getProperty(DataLoadConfig.DATA_SINK_TYPE, "HDFS");

        if (dataSinkType.equalsIgnoreCase("HDFS")) {
            FileSystem fs = new Path(this.configPath).getFileSystem(new Configuration());

            try (FSDataInputStream inputStream = fs.open(new Path(metaFilePath))) {
                this.metaData = inputStream.readUTF();
            }
            dataRootPath = outputPath;
        } else if (dataSinkType.equalsIgnoreCase("VOLUME")) {
            throw new InvalidArgumentException(
                    "Volume only supports load.after.build mode, which is running build, ingest and"
                            + " commit at the same driver.");
        } else if (dataSinkType.equalsIgnoreCase("OSS")) {
            try (OSSFS fs = new OSSFS(properties)) {
                dataRootPath = fs.getQualifiedPath();
                this.metaData = fs.readToString(metaFilePath);
                ingestConfig = fs.getConfig();
            }
        } else {
            throw new InvalidArgumentException("Unsupported data sink: " + dataSinkType);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});
        this.schema = GraphSchemaMapper.parseFromJson(metaMap.get("schema")).toGraphSchema();
        this.columnMappingInfos =
                objectMapper.readValue(
                        metaMap.get("mappings"),
                        new TypeReference<Map<String, ColumnMappingInfo>>() {});
        this.uniquePath = metaMap.get(DataLoadConfig.UNIQUE_PATH);
        dataRootPath = Paths.get(dataRootPath, uniquePath).toString();
    }

    public abstract void run();
}

package com.alibaba.graphscope.groot.dataload;

import com.alibaba.graphscope.compiler.api.schema.GraphSchema;
import com.alibaba.graphscope.groot.dataload.databuild.ColumnMappingInfo;
import com.alibaba.graphscope.groot.dataload.util.Constants;
import com.alibaba.graphscope.groot.dataload.util.OSSFS;
import com.alibaba.graphscope.sdkcommon.schema.GraphSchemaMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
        Properties properties = new Properties();
        try (InputStream is = new FileInputStream(this.configPath)) {
            properties.load(is);
        }
        username = properties.getProperty(Constants.USER_NAME);
        password = properties.getProperty(Constants.PASS_WORD);
        graphEndpoint = properties.getProperty(Constants.GRAPH_ENDPOINT);
        String outputPath = properties.getProperty(Constants.OUTPUT_PATH);
        String metaFilePath = new Path(outputPath, Constants.META_FILE_NAME).toString();
        String dataSinkType = properties.getProperty(Constants.DATA_SINK_TYPE, "HDFS");

        if (dataSinkType.equalsIgnoreCase("HDFS")) {
            FileSystem fs = new Path(this.configPath).getFileSystem(new Configuration());

            try (FSDataInputStream inputStream = fs.open(new Path(metaFilePath))) {
                this.metaData = inputStream.readUTF();
            }
            dataRootPath = outputPath;
        } else if (dataSinkType.equalsIgnoreCase("VOLUME")) {
            throw new IOException(
                    "Volume only supports load.after.build mode, which is running build, ingest and"
                        + " commit at the same driver.");
        } else if (dataSinkType.equalsIgnoreCase("OSS")) {
            try (OSSFS fs = new OSSFS(properties)) {
                dataRootPath = fs.getQualifiedPath();
                this.metaData = fs.readToString(metaFilePath);
                ingestConfig = fs.getConfig();
            }
        } else {
            throw new IOException("Unsupported data sink: " + dataSinkType);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});
        this.schema = GraphSchemaMapper.parseFromJson(metaMap.get("schema")).toGraphSchema();
        this.columnMappingInfos =
                objectMapper.readValue(
                        metaMap.get("mappings"),
                        new TypeReference<Map<String, ColumnMappingInfo>>() {});
        this.uniquePath = metaMap.get(Constants.UNIQUE_PATH);
        dataRootPath = Paths.get(dataRootPath, uniquePath).toString();
    }

    public abstract void run();
}

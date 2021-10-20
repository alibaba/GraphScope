package com.alibaba.maxgraph.dataload;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.dataload.databuild.ColumnMappingInfo;
import com.alibaba.graphscope.groot.schema.GraphSchemaMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public abstract class DataCommand {

    protected String dataPath;
    protected String graphEndpoint;
    protected GraphSchema schema;
    protected Map<String, ColumnMappingInfo> columnMappingInfos;

    public DataCommand(String dataPath) throws IOException {
        this.dataPath = dataPath;
        initialize();
    }

    private void initialize() throws IOException {
        FileSystem fs = new Path(this.dataPath).getFileSystem(new Configuration());
        try (FSDataInputStream inputStream = fs.open(new Path(this.dataPath, "META"))) {
            String metaString = inputStream.readUTF();
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> metaMap = objectMapper.readValue(metaString,
                    new TypeReference<Map<String, String>>() {});
            this.graphEndpoint = metaMap.get("endpoint");
            this.schema = GraphSchemaMapper.parseFromJson(metaMap.get("schema")).toGraphSchema();
            this.columnMappingInfos = objectMapper.readValue(metaMap.get("mappings"),
                    new TypeReference<Map<String, ColumnMappingInfo>>() {});
        }
    }

    public abstract void run();
}

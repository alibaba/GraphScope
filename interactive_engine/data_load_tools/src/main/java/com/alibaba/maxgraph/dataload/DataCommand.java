package com.alibaba.maxgraph.dataload;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.dataload.databuild.ColumnMappingInfo;
import com.alibaba.maxgraph.sdkcommon.schema.GraphSchemaMapper;
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

    protected String dataPath;
    protected String graphEndpoint;
    protected GraphSchema schema;
    protected Map<String, ColumnMappingInfo> columnMappingInfos;
    protected String metaData;
    protected String username;
    protected String password;
    protected String uniquePath;

    protected final String metaFileName = "META";
    protected final String OSS_ENDPOINT = "oss.endpoint";
    protected final String OSS_ACCESS_ID = "oss.access.id";
    protected final String OSS_ACCESS_KEY = "oss.access.key";
    protected final String OSS_BUCKET_NAME = "oss.bucket.name";
    protected final String OSS_OBJECT_NAME = "oss.object.name";
    protected final String USER_NAME = "auth.username";
    protected final String PASS_WORD = "auth.password";

    public DataCommand(String dataPath, boolean isFromOSS, String uniquePath) throws IOException {
        this.dataPath = dataPath;
        this.uniquePath = uniquePath;
        initialize(isFromOSS);
    }

    private void initialize(boolean isFromOSS) throws IOException {
        if (isFromOSS) {
            Properties properties = new Properties();
            try (InputStream is = new FileInputStream(this.dataPath)) {
                properties.load(is);
            } catch (IOException e) {
                throw e;
            }
            String ossEndPoint = properties.getProperty(OSS_ENDPOINT);
            String ossAccessId = properties.getProperty(OSS_ACCESS_ID);
            String ossAccessKey = properties.getProperty(OSS_ACCESS_KEY);
            String ossBucketName = properties.getProperty(OSS_BUCKET_NAME);
            String ossObjectName = properties.getProperty(OSS_OBJECT_NAME);
            username = properties.getProperty(USER_NAME);
            password = properties.getProperty(PASS_WORD);

            dataPath =
                    "oss://"
                            + Paths.get(
                                            Paths.get(ossEndPoint, ossBucketName).toString(),
                                            ossObjectName);

            Map<String, String> ossInfo = new HashMap<String, String>();
            ossInfo.put(OSS_ENDPOINT, ossEndPoint);
            ossInfo.put(OSS_ACCESS_ID, ossAccessId);
            ossInfo.put(OSS_ACCESS_KEY, ossAccessKey);
            OSSFileObj ossFileObj = new OSSFileObj(ossInfo);
            ossObjectName = Paths.get(ossObjectName, uniquePath).toString();

            this.metaData = ossFileObj.readBuffer(ossBucketName, ossObjectName, metaFileName);
            ossFileObj.close();
        } else {
            FileSystem fs = new Path(this.dataPath).getFileSystem(new Configuration());
            try (FSDataInputStream inputStream = fs.open(new Path(this.dataPath, "META"))) {
                this.metaData = inputStream.readUTF();
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});
        this.graphEndpoint = metaMap.get("endpoint");
        this.schema = GraphSchemaMapper.parseFromJson(metaMap.get("schema")).toGraphSchema();
        this.columnMappingInfos =
                objectMapper.readValue(
                        metaMap.get("mappings"),
                        new TypeReference<Map<String, ColumnMappingInfo>>() {});
        this.uniquePath = metaMap.get("unique_path");
    }

    public abstract void run();
}

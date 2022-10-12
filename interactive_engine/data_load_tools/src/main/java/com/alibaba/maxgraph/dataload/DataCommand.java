package com.alibaba.maxgraph.dataload;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.dataload.databuild.ColumnMappingInfo;
import com.alibaba.maxgraph.dataload.util.HttpClient;
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
import java.net.HttpURLConnection;
import java.nio.file.Paths;
import java.util.*;

public abstract class DataCommand {

    protected String configPath;
    protected String graphEndpoint;
    protected GraphSchema schema;
    protected Map<String, ColumnMappingInfo> columnMappingInfos;
    protected String metaData;
    protected String username;
    protected String password;

    protected String ossAccessID;
    protected String ossAccessKey;
    protected String uniquePath;

    protected final String metaFileName = "META";
    protected final String OSS_ENDPOINT = "oss.endpoint";
    protected final String OSS_ACCESS_ID = "oss.access.id";
    protected final String OSS_ACCESS_KEY = "oss.access.key";
    protected final String OSS_BUCKET_NAME = "oss.bucket.name";
    protected final String OSS_OBJECT_NAME = "oss.object.name";
    protected final String OSS_INFO_URL = "oss.info.url";
    protected final String USER_NAME = "auth.username";
    protected final String PASS_WORD = "auth.password";

    public DataCommand(String configPath, boolean isFromOSS, String uniquePath) throws IOException {
        this.configPath = configPath;
        this.uniquePath = uniquePath;
        initialize(isFromOSS);
    }

    private HashMap<String, Object> getOSSInfoFromURL(String URL) throws IOException {
        HttpClient client = new HttpClient();
        HttpURLConnection conn = null;
        try {
            conn = client.createConnection(URL);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef =
                    new TypeReference<HashMap<String, Object>>() {};
            return mapper.readValue(conn.getInputStream(), typeRef);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void initialize(boolean isFromOSS) throws IOException {
        if (isFromOSS) {
            Properties properties = new Properties();
            try (InputStream is = new FileInputStream(this.configPath)) {
                properties.load(is);
            } catch (IOException e) {
                throw e;
            }
            this.ossAccessID = properties.getProperty(OSS_ACCESS_ID);
            this.ossAccessKey = properties.getProperty(OSS_ACCESS_KEY);
            String ossEndPoint = properties.getProperty(OSS_ENDPOINT);
            String ossBucketName = properties.getProperty(OSS_BUCKET_NAME);
            String ossObjectName = properties.getProperty(OSS_OBJECT_NAME);
            if (this.ossAccessID == null) {
                String URL = properties.getProperty(OSS_INFO_URL);
                HashMap<String, Object> o = getOSSInfoFromURL(URL);
                this.ossAccessID = (String) o.get("ossAccessID");
                this.ossAccessKey = (String) o.get("ossAccessKey");
                ossEndPoint = (String) o.get("ossEndpoint");
                ossBucketName = (String) o.get("ossBucketName");
                ossObjectName = (String) o.get("ossObjectName");
            }

            username = properties.getProperty(USER_NAME);
            password = properties.getProperty(PASS_WORD);

            configPath =
                    "oss://"
                            + Paths.get(
                                    Paths.get(ossEndPoint, ossBucketName).toString(),
                                    ossObjectName);

            Map<String, String> ossInfo = new HashMap<>();
            ossInfo.put(OSS_ENDPOINT, ossEndPoint);
            ossInfo.put(OSS_ACCESS_ID, ossAccessID);
            ossInfo.put(OSS_ACCESS_KEY, ossAccessKey);
            OSSFileObj ossFileObj = new OSSFileObj(ossInfo);
            ossObjectName = Paths.get(ossObjectName, uniquePath).toString();

            this.metaData = ossFileObj.readBuffer(ossBucketName, ossObjectName, metaFileName);
            ossFileObj.close();
        } else {
            FileSystem fs = new Path(this.configPath).getFileSystem(new Configuration());
            try (FSDataInputStream inputStream = fs.open(new Path(this.configPath, "META"))) {
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

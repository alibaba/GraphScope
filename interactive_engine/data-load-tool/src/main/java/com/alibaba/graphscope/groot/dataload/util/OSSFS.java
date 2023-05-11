package com.alibaba.graphscope.groot.dataload.util;

import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.oss.*;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.UploadFileRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OSSFS extends AbstractFileSystem {
    private static final Logger logger = LoggerFactory.getLogger(OSSFS.class);

    private String ossAccessID;
    private String ossAccessKey;
    private String endpoint;
    private final String bucket;
    private final String object;

    private OSS ossClient = null;

    private void initClient() throws IOException {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setMaxErrorRetry(10);
        try {
            this.ossClient =
                    new OSSClientBuilder().build(endpoint, ossAccessID, ossAccessKey, conf);
        } catch (OSSException | ClientException oe) {
            throw new IOException(oe);
        }
    }

    public OSSFS(JobConf jobConf) throws IOException {
        this.ossAccessID = jobConf.get(Constants.OSS_ACCESS_ID);
        this.ossAccessKey = jobConf.get(Constants.OSS_ACCESS_KEY);
        this.endpoint = jobConf.get(Constants.OSS_ENDPOINT);
        this.bucket = jobConf.get(Constants.OSS_BUCKET_NAME);
        this.object = jobConf.get(Constants.OSS_OBJECT_NAME);

        if (!endpoint.startsWith("https")) {
            endpoint = "https://" + endpoint;
        }
        initClient();
    }

    public OSSFS(Properties properties) throws IOException {
        this.ossAccessID = properties.getProperty(Constants.OSS_ACCESS_ID);
        this.ossAccessKey = properties.getProperty(Constants.OSS_ACCESS_KEY);
        if (this.ossAccessID == null || this.ossAccessID.isEmpty()) {
            String URL = properties.getProperty(Constants.OSS_INFO_URL);
            HashMap<String, String> o = getOSSInfoFromURL(URL);
            this.ossAccessID = o.get("ossAccessID");
            this.ossAccessKey = o.get("ossAccessKey");
            this.endpoint = o.get("ossEndpoint");
            this.bucket = o.get("ossBucketName");
            this.object = o.get("ossObjectName");
        } else {
            this.endpoint = properties.getProperty(Constants.OSS_ENDPOINT);
            this.bucket = properties.getProperty(Constants.OSS_BUCKET_NAME);
            this.object = properties.getProperty(Constants.OSS_OBJECT_NAME);
        }
        initClient();
    }

    @Override
    public void open(TaskContext context, String mode) throws IOException {}

    public String getQualifiedPath() {
        return "oss://";
    }

    public void setJobConf(JobConf jobConf) {
        jobConf.set(Constants.OSS_ACCESS_ID, ossAccessID);
        jobConf.set(Constants.OSS_ACCESS_KEY, ossAccessKey);
        jobConf.set(Constants.OSS_ENDPOINT, endpoint);
        jobConf.set(Constants.OSS_BUCKET_NAME, bucket);
        jobConf.set(Constants.OSS_OBJECT_NAME, object);
    }

    private HashMap<String, String> getOSSInfoFromURL(String URL) throws IOException {
        HttpClient client = new HttpClient();
        HttpURLConnection conn = null;
        try {
            conn = client.createConnection(URL);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, String>> typeRef =
                    new TypeReference<HashMap<String, String>>() {};
            return mapper.readValue(conn.getInputStream(), typeRef);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public void copy(String srcFile, String dstFile) throws IOException {
        uploadFileWithCheckPoint(srcFile, dstFile);
    }

    public void putObject(String prefix, String fileName) throws IOException {
        String key = Paths.get(object, prefix, fileName).toString();
        try {
            ossClient.putObject(bucket, key, new File(fileName));
        } catch (OSSException | ClientException oe) {
            logger.error("Error message: {}", oe.getMessage());
            throw new IOException(oe);
        }
    }

    public void uploadFileWithCheckPoint(String srcFile, String dstFile) throws IOException {
        String key = Paths.get(object, dstFile).toString();
        UploadFileRequest uploadFileRequest = new UploadFileRequest(bucket, key);
        uploadFileRequest.setUploadFile(srcFile);
        uploadFileRequest.setPartSize(10 * 1024 * 1024);
        uploadFileRequest.setEnableCheckpoint(true);
        try {
            ossClient.uploadFile(uploadFileRequest);
        } catch (OSSException oe) {
            logger.error("Error message: {}", oe.getMessage());
            throw new IOException(oe);
        } catch (Throwable ce) {
            logger.error("Error Message:" + ce.getMessage());
            throw new IOException(ce);
        }
    }

    public void createDirectory(String dirName) throws IOException {}

    public Map<String, String> getConfig() {
        HashMap<String, String> config = new HashMap<>();
        config.put(Constants.OSS_ACCESS_ID, ossAccessID);
        config.put(Constants.OSS_ACCESS_KEY, ossAccessKey);
        config.put(Constants.OSS_ENDPOINT, endpoint);
        config.put(Constants.OSS_BUCKET_NAME, bucket);
        config.put(Constants.OSS_OBJECT_NAME, object);
        return config;
    }

    public String readToString(String fileName) throws IOException {
        StringBuilder data = new StringBuilder();
        String objName = Paths.get(object, fileName).toString();
        try {
            OSSObject ossObject = ossClient.getObject(bucket, objName);

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                data.append(line);
            }
            reader.close();
            ossObject.close();
        } catch (OSSException | ClientException oe) {
            logger.error("Error Message:" + oe.getMessage());
            throw new IOException(oe);
        }
        return data.toString();
    }

    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}

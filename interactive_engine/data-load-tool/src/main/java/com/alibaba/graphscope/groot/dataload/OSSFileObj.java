package com.alibaba.graphscope.groot.dataload;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CreateDirectoryRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadFileRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Map;

public class OSSFileObj {
    protected final String OSS_ENDPOINT = "oss.endpoint";
    protected final String OSS_ACCESS_ID = "oss.access.id";
    protected final String OSS_ACCESS_KEY = "oss.access.key";

    protected String ossEndpoint = null;
    protected String ossAccessID = null;
    protected String ossAccessKey = null;

    protected OSS ossClient = null;

    public OSSFileObj(Map<String, String> ossInfo) throws IOException {
        this.ossEndpoint = ossInfo.get(OSS_ENDPOINT);
        this.ossAccessID = ossInfo.get(OSS_ACCESS_ID);
        this.ossAccessKey = ossInfo.get(OSS_ACCESS_KEY);

        if (!ossEndpoint.startsWith("http")) {
            ossEndpoint = "https://" + ossEndpoint;
        }

        try {
            this.ossClient = new OSSClientBuilder().build(ossEndpoint, ossAccessID, ossAccessKey);
        } catch (OSSException oe) {
            throw new IOException(oe);
        } catch (ClientException ce) {
            throw new IOException(ce);
        }
    }

    public void uploadFile(String ossBucketName, String ossObjectName, String fileName)
            throws IOException {
        try {
            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(
                            ossBucketName,
                            Paths.get(ossObjectName, fileName).toString(),
                            new File(fileName));
            ossClient.putObject(putObjectRequest);
        } catch (OSSException oe) {
            throw new IOException(oe);
        } catch (ClientException ce) {
            throw new IOException(ce);
        }
    }

    public void uploadFileWithCheckPoint(
            String ossBucketName, String ossObjectName, String fileName) throws IOException {
        try {
            UploadFileRequest uploadFileRequest =
                    new UploadFileRequest(
                            ossBucketName, Paths.get(ossObjectName, fileName).toString());
            uploadFileRequest.setUploadFile(fileName);
            uploadFileRequest.setPartSize(10 * 1024 * 1024);
            uploadFileRequest.setEnableCheckpoint(true);
            ossClient.uploadFile(uploadFileRequest);
        } catch (OSSException oe) {
            throw new IOException(oe);
        } catch (ClientException ce) {
            throw new IOException(ce);
        } catch (Throwable ce) {
            throw new IOException(ce);
        }
    }

    public void createDirectory(String ossBucketName, String ossObjectName, String dirName)
            throws IOException {
        try {
            String directoryName = Paths.get(ossObjectName, dirName).toString();
            CreateDirectoryRequest createDirectoryRequest =
                    new CreateDirectoryRequest(ossBucketName, directoryName);
            ossClient.createDirectory(createDirectoryRequest);
        } catch (OSSException oe) {
            throw new IOException(oe);
        } catch (ClientException ce) {
            throw new IOException(ce);
        }
    }

    public String readBuffer(String ossBucketName, String ossObjectName, String fileName)
            throws IOException {
        String data = "";
        try {
            OSSObject ossObject =
                    ossClient.getObject(
                            ossBucketName, Paths.get(ossObjectName, fileName).toString());

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                data += line;
            }
            reader.close();
            ossObject.close();
        } catch (OSSException oe) {
            throw new IOException(oe);
        } catch (ClientException ce) {
            throw new IOException(ce);
        }
        return data;
    }

    public void close() throws IOException {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }
}

package com.alibaba.graphscope.groot.store.external;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;

import java.io.File;
import java.net.URI;
import java.util.Map;

public class OssStorage extends ExternalStorage {

    private OSS ossClient;

    public OssStorage(String path, Map<String, String> config) {
        URI uri = URI.create(path);
        String endpoint = uri.getAuthority();
        String accessID = config.get("ossAccessID");
        String accessKey = config.get("ossAccessKey");
        this.ossClient = new OSSClientBuilder().build(endpoint, accessID, accessKey);
    }

    @Override
    public void downloadData(String srcPath, String dstPath) {
        URI uri = URI.create(srcPath);
        String[] pathItems = uri.getPath().split("/", 3);
        String bucketName = pathItems[1];
        String objectName = pathItems[2];
        ossClient.getObject(new GetObjectRequest(bucketName, objectName), new File(dstPath));
    }
}

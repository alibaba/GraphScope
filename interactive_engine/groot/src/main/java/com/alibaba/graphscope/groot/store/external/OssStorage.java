package com.alibaba.graphscope.groot.store.external;

import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import java.io.File;
import java.net.URI;

public class OssStorage extends ExternalStorage {

    private OSS ossClient;

    public OssStorage(Configs configs, String path) {
        URI uri = URI.create(path);
        String accessId = StoreConfig.OSS_ACCESS_ID.get(configs);
        String accessSecret = StoreConfig.OSS_ACCESS_SECRET.get(configs);
        String endpoint = uri.getAuthority();
        this.ossClient = new OSSClientBuilder().build(endpoint, accessId, accessSecret);
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

/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.store.external;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

public class OssStorage extends ExternalStorage {
    private static final Logger logger = LoggerFactory.getLogger(OssStorage.class);
    private final OSS ossClient;
    private String bucket;
    private String rootPath;

    public OssStorage(String path, Map<String, String> config) {
        String endpoint = config.get("oss.endpoint");
        String accessID = config.get("oss.access.id");
        String accessKey = config.get("oss.access.key");
        bucket = config.get("oss.bucket.name");
        rootPath = config.get("oss.object.name");
        this.ossClient = new OSSClientBuilder().build(endpoint, accessID, accessKey);
    }

    @Override
    public void downloadDataSimple(String srcPath, String dstPath) {
        logger.info("Downloading " + srcPath + " to " + dstPath);
        String[] pathItems = srcPath.split("://");
        String objectFullName = Paths.get(rootPath, pathItems[1]).toString();
        ossClient.getObject(new GetObjectRequest(bucket, objectFullName), new File(dstPath));
    }
}

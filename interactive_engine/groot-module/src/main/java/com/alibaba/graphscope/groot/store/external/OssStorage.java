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

import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class OssStorage extends ExternalStorage {

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

    public void downloadDataSimple(String srcPath, String dstPath) {
        String[] pathItems = srcPath.split("://");
        String objectFullName = Paths.get(rootPath, pathItems[1]).toString();
        ossClient.getObject(new GetObjectRequest(bucket, objectFullName), new File(dstPath));
    }

    @Override
    public void downloadData(String srcPath, String dstPath) throws IOException {
        // Check chk
        String chkPath = srcPath.substring(0, srcPath.length() - ".sst".length()) + ".chk";
        String chkLocalPath =
                dstPath.substring(0, srcPath.length() - ".sst".length()) + ".chk";

        downloadDataSimple(chkPath, chkLocalPath);
        File chkFile = new File(chkLocalPath);
        byte[] chkData = new byte[(int) chkFile.length()];
        try {
            FileInputStream fis = new FileInputStream(chkFile);
            fis.read(chkData);
            fis.close();
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
        String[] chkArray = new String(chkData).split(",");
        if ("0".equals(chkArray[0])) {
            return;
        }
        String chkMD5Value = chkArray[1];
        downloadDataSimple(srcPath, dstPath);
        String sstMD5Value = getFileMD5(dstPath);
        if (!chkMD5Value.equals(sstMD5Value)) {
            throw new IOException("CheckSum failed for " + srcPath);
        } else {
            // The .chk file are now useless
            chkFile.delete();
        }
    }
}

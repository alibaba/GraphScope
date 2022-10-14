/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.dataload.OSSFileObj;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataBuildReducerOdps extends ReducerBase {
    private String ossAccessID = null;
    private String ossAccessKey = null;
    private String ossEndpoint = null;
    private String ossBucketName = null;
    private String ossObjectName = null;

    private SstRecordWriter sstRecordWriter = null;
    private String uniquePath = null;
    private String taskId = null;
    private String metaData = null;
    private OSSFileObj ossFileObj = null;
    private String sstFileName = null;
    private String chkFileName = null;
    private String metaFileName = "META";
    private boolean sstFileEmpty;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.ossAccessID = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_ID);
        this.ossAccessKey = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_KEY);
        this.ossEndpoint = context.getJobConf().get(OfflineBuildOdps.OSS_ENDPOINT);
        this.ossBucketName = context.getJobConf().get(OfflineBuildOdps.OSS_BUCKET_NAME);
        this.ossObjectName = context.getJobConf().get(OfflineBuildOdps.OSS_OBJECT_NAME);
        this.metaData = context.getJobConf().get(OfflineBuildOdps.META_INFO);
        this.uniquePath = context.getJobConf().get(OfflineBuildOdps.UNIQUE_PATH);

        if (!ossEndpoint.startsWith("http")) {
            ossEndpoint = "https://" + ossEndpoint;
        }

        this.taskId = context.getTaskID().toString();
        taskId = taskId.substring(taskId.length() - 5);
        sstFileName = "part-r-" + taskId + ".sst";
        chkFileName = "part-r-" + taskId + ".chk";

        Map<String, String> ossInfo = new HashMap<String, String>();
        ossInfo.put(OfflineBuildOdps.OSS_ENDPOINT, ossEndpoint);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_ID, ossAccessID);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_KEY, ossAccessKey);

        this.ossFileObj = new OSSFileObj(ossInfo);

        /*
        if ("00000".equals(taskId)) {
            try {
                ossFileObj.createDirectory(ossBucketName, ossObjectName, uniquePath);
            } catch (IOException e) {
                throw e;
            }
        }
        */

        ossObjectName = Paths.get(ossObjectName, uniquePath).toString();

        try {
            this.sstRecordWriter = new SstRecordWriter(sstFileName, DataBuildMapperOdps.charSet);
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
            throws IOException {
        while (values.hasNext()) {
            Record value = values.next();
            try {
                sstRecordWriter.write((String) key.get(0), (String) value.get(0));
            } catch (IOException e) {
                throw e;
            }
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
        this.sstFileEmpty = sstRecordWriter.empty();
        try {
            sstRecordWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String chkData = null;
        if (sstFileEmpty) {
            chkData = "0";
        } else {
            chkData = "1," + getFileMD5(sstFileName);
        }
        writeFile(chkFileName, chkData);
        ossFileObj.uploadFile(ossBucketName, ossObjectName, chkFileName);
        if (!sstFileEmpty) {
            ossFileObj.uploadFileWithCheckPoint(ossBucketName, ossObjectName, sstFileName);
        }

        // Only the first task will write  the meta
        if ("00000".equals(taskId)) {
            try {
                writeFile(metaFileName, metaData);
                ossFileObj.uploadFile(ossBucketName, ossObjectName, metaFileName);
            } catch (IOException e) {
                throw e;
            }
        }

        ossFileObj.close();
    }

    public String getFileMD5(String fileName) throws IOException {
        FileInputStream fis = null;
        try {
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            fis = new FileInputStream(fileName);
            byte[] buffer = new byte[8192];
            int length;
            while ((length = fis.read(buffer)) != -1) {
                MD5.update(buffer, 0, length);
            }
            return new String(Hex.encodeHex(MD5.digest()));
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                throw e;
            }
        }
    }

    public void writeFile(String fileName, String data) throws IOException {
        File file = new File(fileName);
        FileOutputStream fos = new FileOutputStream(file);
        if (!file.exists()) {
            file.createNewFile();
        }
        fos.write(data.getBytes());
        fos.flush();
        fos.close();
    }
}

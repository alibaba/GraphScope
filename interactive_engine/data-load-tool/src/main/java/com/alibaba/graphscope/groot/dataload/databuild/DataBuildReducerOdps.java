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
package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.groot.dataload.util.AbstractFileSystem;
import com.alibaba.graphscope.groot.dataload.util.Constants;
import com.alibaba.graphscope.groot.dataload.util.FSFactory;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;

public class DataBuildReducerOdps extends ReducerBase {
    private SstRecordWriter sstRecordWriter = null;

    private String uniquePath = null;
    private String taskId = null;
    private String metaData = null;
    private AbstractFileSystem fs = null;
    private String sstFileName = null;
    private String chkFileName = null;
    private String metaFileName = Constants.META_FILE_NAME;

    @Override
    public void setup(TaskContext context) throws IOException {

        metaData = context.getJobConf().get(Constants.META_INFO);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});

        this.uniquePath = metaMap.get(Constants.UNIQUE_PATH);

        this.taskId = context.getTaskID().toString();
        taskId = taskId.substring(taskId.length() - 5);
        sstFileName = "part-r-" + taskId + ".sst";
        chkFileName = "part-r-" + taskId + ".chk";

        this.fs = FSFactory.Create(context.getJobConf());
        this.fs.open(context, "w");
        /*
        if ("00000".equals(taskId)) {
            try {
                ossFileObj.createDirectory(ossBucketName, ossObjectName, uniquePath);
            } catch (IOException e) {
                throw e;
            }
        }
        */

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
            sstRecordWriter.write((String) key.get(0), (String) value.get(0));
        }
        context.progress();
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
        boolean sstFileEmpty = sstRecordWriter.empty();
        try {
            sstRecordWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String chkData = sstFileEmpty ? "0" : "1," + getFileMD5(sstFileName);
        writeFile(chkFileName, chkData);

        fs.copy(chkFileName, Paths.get(uniquePath, chkFileName).toString());
        if (!sstFileEmpty) {
            fs.copy(sstFileName, Paths.get(uniquePath, sstFileName).toString());
        }

        // Only the first task will write the meta
        if ("00000".equals(taskId)) {
            try {
                writeFile(metaFileName, metaData);
                fs.copy(metaFileName, Paths.get(uniquePath, metaFileName).toString());
            } catch (IOException e) {
                throw e;
            }
        }

        fs.close();
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
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IOException(e);
        } finally {
            if (fis != null) {
                fis.close();
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

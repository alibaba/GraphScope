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


import com.aliyun.oss.ClientException;
import com.aliyun.oss.*;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.aliyun.odps.io.BytesWritable;
import java.io.IOException;
import java.util.Iterator;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;


public class DataBuildReducerOdps extends ReducerBase {
    private Record result = null;
    private String ossAccessId = null;
    private String ossAccessKey = null;
    private String ossEndPoint = null;
    private String ossBucketName = null;
    private String ossObjectName = null;
    private OSS ossClient = null;

    private AppendObjectRequest appendObjectRequest = null;
    private ObjectMetadata meta = null;
    private String content1 = null;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.result = context.createOutputRecord();
        this.ossAccessId = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_ID);
        this.ossAccessKey = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_KEY);
        this.ossEndPoint = context.getJobConf().get(OfflineBuildOdps.OSS_ENDPOINT);
        this.ossBucketName = context.getJobConf().get(OfflineBuildOdps.OSS_BUCKET_NAME);
        this.ossObjectName = context.getJobConf().get(OfflineBuildOdps.OSS_OBJECT_NAME);

        this.ossClient = new OSSClientBuilder().build(ossEndPoint, ossAccessId, ossAccessKey);
 

        meta = new ObjectMetadata();
        meta.setContentType("text/plain");
        meta.setCacheControl("no-cache");
        meta.setContentDisposition("attachment;filename=part-r-00000.sst");

        content1 = new String("content1");
        appendObjectRequest = new AppendObjectRequest(this.ossBucketName, this.ossObjectName,
                              new ByteArrayInputStream(content1.getBytes()), meta);
        appendObjectRequest.setBucketName(this.ossBucketName);
        appendObjectRequest.setKey(this.ossObjectName);
        appendObjectRequest.setMetadata(meta);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
        AppendObjectResult appendObjectResult = null;
        while (values.hasNext()) {
            Record value = values.next();
            result.set(0, "");
            result.set(1, "");
            try {
                if (appendObjectResult == null) {
                    appendObjectRequest.setPosition(0L);
                }
                else {
                    appendObjectRequest.setPosition(appendObjectResult.getNextPosition());
                }
                byte[] keyBytes = ((String)(key.get(0))).getBytes();
                byte[] valueBytes = ((String)(value.get(0))).getBytes();
                byte[] newBytes = new byte[keyBytes.length + valueBytes.length];
                System.arraycopy(keyBytes, 0, newBytes, 0, keyBytes.length);
                System.arraycopy(valueBytes, 0, newBytes, keyBytes.length, valueBytes.length);
                appendObjectRequest.setInputStream(new ByteArrayInputStream(newBytes));
                appendObjectResult = ossClient.appendObject(appendObjectRequest);
            } catch (OSSException oe) {
                System.out.println("Caught an OSSException, which means your request made it to OSS, "
                        + "but was rejected with an error response for some reason.");
                System.out.println("Error Message:" + oe.getErrorMessage());
                System.out.println("Error Code:" + oe.getErrorCode());
                System.out.println("Request ID:" + oe.getRequestId());
                System.out.println("Host ID:" + oe.getHostId());
            } catch (ClientException ce) {
                System.out.println("Caught an ClientException, which means the client encountered "
                        + "a serious internal problem while trying to communicate with OSS, "
                        + "such as not being able to access the network.");
                System.out.println("Error Message:" + ce.getMessage());
            }
        }
        if (this.ossClient != null) {
            this.ossClient.shutdown();
        }
    }
}

/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.dataload.graph;

import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.alibaba.graphscope.dataload.jna.ExprGraphStoreLibrary;
import com.alibaba.graphscope.dataload.jna.type.ResultCode;
import com.alibaba.maxgraph.dataload.OSSFileObj;
import com.alibaba.maxgraph.dataload.databuild.OfflineBuildOdps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class IrWriteGraphReducer extends ReducerBase {
    private static final ExprGraphStoreLibrary LIB = ExprGraphStoreLibrary.INSTANCE;
    private Pointer graphLoader;
    private String localRootDir = "/tmp";
    private int taskId;
    private OSSFileObj ossFileObj;
    private String ossBucketName;
    private String ossObjectPrefix;
    private IrVertexData vertexData;
    private IrEdgeData edgeData;
    private int partitions;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.partitions = Integer.valueOf(context.getJobConf().get(IrDataBuild.GRAPH_REDUCER_NUM));
        this.taskId = context.getTaskID().getInstId();
        this.graphLoader = LIB.initGraphLoader(this.localRootDir, taskId);

        this.vertexData = new IrVertexData();
        this.edgeData = new IrEdgeData();

        this.ossBucketName = context.getJobConf().get(OfflineBuildOdps.OSS_BUCKET_NAME);
        this.ossObjectPrefix = context.getJobConf().get(IrDataBuild.WRITE_GRAPH_OSS_PATH);

        String ossAccessId = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_ID);
        String ossAccessKey = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_KEY);
        String ossEndPoint = context.getJobConf().get(OfflineBuildOdps.OSS_ENDPOINT);

        Map<String, String> ossInfo = new HashMap();
        ossInfo.put(OfflineBuildOdps.OSS_ENDPOINT, ossEndPoint);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_ID, ossAccessId);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_KEY, ossAccessKey);

        this.ossFileObj = new OSSFileObj(ossInfo);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) {
        int type = key.getBigint(1).intValue();
        List<Record> records = new ArrayList<>();
        while (values.hasNext()) {
            records.add(values.next().clone());
        }
        if (type == 0) { // write vertex
            Pointer buffer = LIB.initWriteVertex(records.size());
            records.forEach(
                    k -> {
                        this.vertexData.readRecord(k);
                        if (!LIB.writeVertex(buffer, this.vertexData.toFfiVertexData())) {
                            throw new RuntimeException("write one vertex fail");
                        }
                    });
            int resCnt = LIB.finalizeWriteVertex(this.graphLoader, buffer);
            if (resCnt != records.size()) {
                throw new RuntimeException(
                        "write vertices "
                                + resCnt
                                + " but should be "
                                + records.size()
                                + " data is ["
                                + this.vertexData
                                + "]");
            }
        } else if (type == 1) { // write edge
            Pointer buffer = LIB.initWriteEdge(records.size());
            records.forEach(
                    k -> {
                        this.edgeData.readRecord(k);
                        if (!LIB.writeEdge(buffer, this.edgeData.toFfiEdgeData())) {
                            throw new RuntimeException("write one edge fail");
                        }
                    });
            int resCnt =
                    LIB.finalizeWriteEdge(this.graphLoader, buffer, this.taskId, this.partitions);
            if (resCnt != records.size()) {
                throw new RuntimeException(
                        "write edges "
                                + resCnt
                                + " but should be "
                                + records.size()
                                + " data is ["
                                + this.edgeData
                                + "]");
            }
        } else {
            throw new RuntimeException(
                    "element type should be vertex(0) or edge(1), but is " + type);
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
        super.cleanup(context);
        if (this.graphLoader != null) {
            ResultCode code = LIB.finalizeGraphLoading(this.graphLoader);
            if (code != ResultCode.Success) {
                throw new RuntimeException("finalize graph loading fail, code is " + code);
            }
        }
        List<String> graphPaths = getWriteGraphPaths(this.taskId);
        for (String graphPath : graphPaths) {
            String ossObjectName = Paths.get(this.ossObjectPrefix, graphPath).toString();
            String localPath = Paths.get(this.localRootDir, graphPath).toString();
            this.ossFileObj.uploadFileWithCheckPoint(
                    this.ossBucketName, ossObjectName, new File(localPath));
        }
        this.ossFileObj.close();
        File file = new File(Paths.get(localRootDir, "graph_data_bin").toString());
        if (file.exists() && file.isDirectory()) {
            file.delete();
        }
    }

    private List<String> getWriteGraphPaths(int partitionId) {
        String dir = "graph_data_bin";
        String partition = "partition_" + partitionId;
        List<String> fileNames =
                Arrays.asList("edge_property", "graph_struct", "index_data", "node_property");
        return fileNames.stream()
                .map(k -> Paths.get(dir, partition, k).toString())
                .collect(Collectors.toList());
    }
}

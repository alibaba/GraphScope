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

package com.alibaba.graphscope.dataload.encode;

import com.alibaba.graphscope.dataload.ColumnMappingMeta;
import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.alibaba.graphscope.dataload.jna.ExprGraphStoreLibrary;
import com.alibaba.graphscope.dataload.jna.type.FfiEdgeData;
import com.alibaba.graphscope.dataload.jna.type.FfiEdgeTypeTuple;
import com.alibaba.graphscope.dataload.jna.type.FfiVertexData;
import com.alibaba.graphscope.dataload.jna.type.ResultCode;
import com.alibaba.maxgraph.dataload.OSSFileObj;
import com.alibaba.maxgraph.dataload.databuild.OfflineBuildOdps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jna.Pointer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class IrDataEncodeMapper extends MapperBase {
    private static final ExprGraphStoreLibrary LIB = ExprGraphStoreLibrary.INSTANCE;
    private Record vertexRecord;
    private Record edgeRecord;
    private String vertexOutTable;
    private String edgeOutTable;
    private ColumnMappingMeta columnMappingMeta;
    private Pointer parser;
    private IrVertexData vertexData;
    private IrEdgeData edgeData;

    private String localRootDir = "/tmp";

    private String ossBucketName;
    private String ossObjectPrefix;
    private OSSFileObj ossFileObj;

    private int taskId;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.taskId = context.getTaskID().getInstId();
        String mappingJson = context.getJobConf().get(IrDataBuild.COLUMN_MAPPING_META);
        this.parser = LIB.initParserFromJson(mappingJson);
        this.columnMappingMeta =
                (new ObjectMapper())
                        .readValue(mappingJson, new TypeReference<ColumnMappingMeta>() {})
                        .init();
        String outputPrefix = context.getJobConf().get(IrDataBuild.UNIQUE_NAME);
        int partitions =
                Integer.valueOf(context.getJobConf().get(IrDataBuild.ENCODE_OUTPUT_TABLE_NUM));
        int taskId = context.getTaskID().getInstId();
        this.vertexOutTable =
                outputPrefix + IrDataBuild.ENCODE_VERTEX_MAGIC + (taskId % partitions);
        this.edgeOutTable = outputPrefix + IrDataBuild.ENCODE_EDGE_MAGIC + (taskId % partitions);
        // table name as label
        this.vertexRecord = context.createOutputRecord(this.vertexOutTable);
        this.edgeRecord = context.createOutputRecord(this.edgeOutTable);
        this.vertexData = new IrVertexData();
        this.edgeData = new IrEdgeData();

        this.ossBucketName = context.getJobConf().get(OfflineBuildOdps.OSS_BUCKET_NAME);
        this.ossObjectPrefix = context.getJobConf().get(IrDataBuild.UNIQUE_NAME);

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
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        char separator = context.getJobConf().get(IrDataBuild.SEPARATOR).charAt(0);
        String tableName = context.getInputTableInfo().getTableName();
        String content = ignoreQuote(record.getString(0));
        String labelName = this.columnMappingMeta.getLabelName(tableName);
        long type = this.columnMappingMeta.getElementType(tableName);
        if (type == 0) { // encode vertex
            Pointer vertexParser = LIB.getVertexParser(parser, labelName);
            FfiVertexData.ByValue data = LIB.encodeVertex(vertexParser, content, separator);
            if (data.code != ResultCode.Success) {
                throw new RuntimeException("encode ffi vertex data fail, content is " + content);
            }
            this.vertexData.toIrVertexData(data).writeRecord(this.vertexRecord);
            context.write(this.vertexRecord, this.vertexOutTable);
            LIB.destroyVertexParser(vertexParser);
            data.propertyBytes.close();
        } else { // encode edge
            String srcLabel = this.columnMappingMeta.getSrcLabel(tableName);
            String dstLabel = this.columnMappingMeta.getDstLabel(tableName);
            Pointer edgeParser =
                    LIB.getEdgeParser(
                            parser, new FfiEdgeTypeTuple.ByValue(labelName, srcLabel, dstLabel));
            FfiEdgeData.ByValue data = LIB.encodeEdge(edgeParser, content, separator);
            if (data.code != ResultCode.Success) {
                throw new RuntimeException("encode ffi edge data fail, content is " + content);
            }
            this.edgeData.toIrEdgeData(data).writeRecord(this.edgeRecord);
            context.write(this.edgeRecord, this.edgeOutTable);
            LIB.destroyEdgeParser(edgeParser);
            data.propertyBytes.close();
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
        super.cleanup(context);
        if (this.taskId == 0) {
            String schemaJson = LIB.getSchemaJsonFromParser(this.parser);
            String ossObjectName =
                    Paths.get(this.ossObjectPrefix, "graph_schema", "schema.json").toString();
            String localPath = Paths.get(localRootDir, "graph_schema", "schema.json").toString();
            File localFile = new File(localPath);
            FileUtils.writeStringToFile(localFile, schemaJson, StandardCharsets.UTF_8);
            this.ossFileObj.uploadFileWithCheckPoint(this.ossBucketName, ossObjectName, localFile);

            if (localFile.exists()) {
                localFile.delete();
            }
        }
        if (this.parser != null) {
            LIB.destroyParser(this.parser);
        }
    }

    private String ignoreQuote(String content) {
        return content.replace("\"", "");
    }
}

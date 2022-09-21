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
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jna.Pointer;

import java.io.IOException;

public class IrDataEncodeMapper extends MapperBase {
    private Record vertexRecord;
    private Record edgeRecord;
    private String vertexOutTable;
    private String edgeOutTable;
    private ColumnMappingMeta columnMappingMeta;
    private Pointer parser;
    private IrVertexData vertexData;
    private IrEdgeData edgeData;
    private static final ExprGraphStoreLibrary LIB = ExprGraphStoreLibrary.INSTANCE;

    @Override
    public void setup(TaskContext context) throws IOException {
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
        if (this.parser != null) {
            LIB.destroyParser(this.parser);
        }
    }

    private String ignoreQuote(String content) {
        return content.replace("\"", "");
    }
}

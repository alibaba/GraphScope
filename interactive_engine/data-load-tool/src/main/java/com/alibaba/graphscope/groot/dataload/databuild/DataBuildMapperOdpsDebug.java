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

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.schema.mapper.GraphSchemaMapper;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
import com.alibaba.graphscope.groot.common.util.SchemaUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DataBuildMapperOdpsDebug extends MapperBase {
    private static final Logger logger = LoggerFactory.getLogger(DataBuildMapperOdpsDebug.class);
    private GraphSchema graphSchema;
    private DataEncoder dataEncoder;
    private Map<String, ColumnMappingInfo> fileToColumnMappingInfo;

    private Record outKey;

    @Override
    public void setup(TaskContext context) throws IOException {
        outKey = context.createOutputRecord();

        String metaData = context.getJobConf().get(DataLoadConfig.META_INFO);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});
        String schemaJson = metaMap.get(DataLoadConfig.SCHEMA_JSON);
        graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        dataEncoder = new DataEncoder(graphSchema);
        String columnMappingsJson = metaMap.get(DataLoadConfig.COLUMN_MAPPINGS);
        fileToColumnMappingInfo =
                objectMapper.readValue(
                        columnMappingsJson, new TypeReference<Map<String, ColumnMappingInfo>>() {});
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        TableInfo tableInfo = context.getInputTableInfo();
        String identifier = tableInfo.getTableName() + "|" + tableInfo.getPartPath();
        ColumnMappingInfo info = this.fileToColumnMappingInfo.get(identifier);
        if (info == null) {
            logger.warn("Mapper: ignore [{}], table info: [{}]", identifier, tableInfo);
            return;
        }

        String[] items = Utils.parseRecords(record);

        int labelId = info.getLabelId();
        long tableId = info.getTableId();
        GraphElement type = this.graphSchema.getElement(labelId);
        Map<Integer, Integer> colMap = info.getPropertiesColMap();
        Map<Integer, PropertyValue> properties = Utils.buildProperties(type, items, colMap);
        if (type instanceof GraphVertex) {
            outKey.set(1, getVertexRawKeys((GraphVertex) type, colMap, items));
            BytesRef keyRef =
                    Utils.getVertexKeyRef(dataEncoder, (GraphVertex) type, properties, tableId);
            outKey.set(0, getVertexKeyEncoded(keyRef));
            context.write(outKey);
        } else if (type instanceof GraphEdge) {
            outKey.set(1, getEdgeRawKeys(info, items));
            BytesRef out =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, true);
            outKey.set(0, getEdgeKeyEncoded(out));
            context.write(outKey);
            BytesRef in =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, false);
            outKey.set(0, getEdgeKeyEncoded(in));
            context.write(outKey);
        } else {
            throw new InvalidArgumentException("Invalid label " + labelId);
        }
    }

    private String getVertexRawKeys(GraphVertex type, Map<Integer, Integer> colMap, String[] items)
            throws IOException {
        List<Integer> pkIds = SchemaUtils.getVertexPrimaryKeyList(type);
        List<Integer> indices = new ArrayList<>();
        colMap.forEach(
                (idx, propId) -> {
                    if (pkIds.contains(propId)) {
                        indices.add(idx);
                    }
                });
        return concatenateItemsByIndices(items, indices);
    }

    private String getEdgeRawKeys(ColumnMappingInfo info, String[] items) {
        Map<Integer, Integer> srcPkColMap = info.getSrcPkColMap();
        Map<Integer, Integer> dstPkColMap = info.getDstPkColMap();

        List<Integer> pkIds = new ArrayList<>(srcPkColMap.keySet());
        pkIds.addAll(dstPkColMap.keySet());
        return concatenateItemsByIndices(items, pkIds);
    }

    private static String concatenateItemsByIndices(String[] array, List<Integer> indices) {
        StringBuilder builder = new StringBuilder();
        if (indices.isEmpty()) {
            throw new InvalidArgumentException("indices are empty!");
        }
        for (int index : indices) {
            builder.append(array[index]).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    private String getVertexKeyEncoded(BytesRef keyRef) {
        ByteBuffer buffer = ByteBuffer.wrap(keyRef.getBytes());
        long tableId = buffer.getLong(0);
        long hashId = buffer.getLong(8);
        return tableId + "/" + hashId;
    }

    private String getEdgeKeyEncoded(BytesRef keyRef) {
        ByteBuffer buffer = ByteBuffer.wrap(keyRef.getBytes());
        long tableId = buffer.getLong(0);
        long srcHashId = buffer.getLong(8);
        long dstHashId = buffer.getLong(16);
        long eid = buffer.getLong(24);
        return tableId + "/" + srcHashId + "/" + dstHashId + "/" + eid;
    }
}

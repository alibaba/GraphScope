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
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataBuildMapperOdps extends MapperBase {
    private static final Logger logger = LoggerFactory.getLogger(DataBuildMapperOdps.class);
    public static final SimpleDateFormat DST_FMT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    public static String charSet = "ISO8859-1";

    private GraphSchema graphSchema;
    private DataEncoder dataEncoder;
    private Map<String, ColumnMappingInfo> fileToColumnMappingInfo;

    private Record outKey;
    private Record outVal;

    @Override
    public void setup(TaskContext context) throws IOException {
        outKey = context.createMapOutputKeyRecord();
        outVal = context.createMapOutputValueRecord();

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
        DST_FMT.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
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

        BytesRef valRef = this.dataEncoder.encodeProperties(labelId, properties);
        outVal.set(new Object[] {new String(valRef.getBytes(), charSet)});
        if (type instanceof GraphVertex) {
            BytesRef keyRef =
                    Utils.getVertexKeyRef(dataEncoder, (GraphVertex) type, properties, tableId);
            outKey.set(new Object[] {new String(keyRef.getBytes(), charSet)});
            context.write(outKey, outVal);
        } else if (type instanceof GraphEdge) {
            BytesRef out =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, true);
            outKey.set(new Object[] {new String(out.getBytes(), charSet)});
            context.write(outKey, outVal);
            BytesRef in =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, false);
            outKey.set(new Object[] {new String(in.getBytes(), charSet)});
            context.write(outKey, outVal);
        } else {
            throw new InvalidArgumentException("Invalid label " + labelId);
        }
    }
}

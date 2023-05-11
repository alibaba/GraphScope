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

import com.alibaba.graphscope.compiler.api.exception.PropertyDefNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.*;
import com.alibaba.graphscope.groot.dataload.util.Constants;
import com.alibaba.graphscope.sdkcommon.schema.GraphSchemaMapper;
import com.alibaba.graphscope.sdkcommon.schema.PropertyValue;
import com.aliyun.odps.data.Record;
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

        String metaData = context.getJobConf().get(Constants.META_INFO);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> metaMap =
                objectMapper.readValue(metaData, new TypeReference<Map<String, String>>() {});
        String schemaJson = metaMap.get(Constants.SCHEMA_JSON);
        graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        dataEncoder = new DataEncoder(graphSchema);
        String columnMappingsJson = metaMap.get(Constants.COLUMN_MAPPINGS);
        fileToColumnMappingInfo =
                objectMapper.readValue(
                        columnMappingsJson, new TypeReference<Map<String, ColumnMappingInfo>>() {});
        DST_FMT.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String tableName = context.getInputTableInfo().getTableName();
        ColumnMappingInfo columnMappingInfo = this.fileToColumnMappingInfo.get(tableName);
        if (columnMappingInfo == null) {
            logger.warn(
                    "Mapper: ignore [{}], table info is [{}]",
                    tableName,
                    context.getInputTableInfo());
            return;
        }

        int labelId = columnMappingInfo.getLabelId();
        long tableId = columnMappingInfo.getTableId();
        int columnCount = record.getColumnCount();
        String[] items = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            if (record.get(i) == null) {
                items[i] = "";
            } else {
                items[i] = record.get(i).toString();
            }
        }
        Map<Integer, Integer> propertiesColumnMapping = columnMappingInfo.getPropertiesColMap();
        GraphElement type = this.graphSchema.getElement(labelId);
        Map<Integer, PropertyValue> propertiesMap =
                buildPropertiesMap(type, items, propertiesColumnMapping);
        BytesRef valRef = this.dataEncoder.encodeProperties(labelId, propertiesMap);
        outVal.set(new Object[] {new String(valRef.getBytes(), charSet)});
        if (type instanceof GraphVertex) {
            BytesRef keyRef =
                    this.dataEncoder.encodeVertexKey((GraphVertex) type, propertiesMap, tableId);
            outKey.set(new Object[] {new String(keyRef.getBytes(), charSet)});
            context.write(outKey, outVal);
        } else if (type instanceof GraphEdge) {
            int srcLabelId = columnMappingInfo.getSrcLabelId();
            Map<Integer, Integer> srcPkColMap = columnMappingInfo.getSrcPkColMap();
            GraphElement srcType = this.graphSchema.getElement(srcLabelId);
            Map<Integer, PropertyValue> srcPkMap = buildPropertiesMap(srcType, items, srcPkColMap);

            int dstLabelId = columnMappingInfo.getDstLabelId();
            Map<Integer, Integer> dstPkColMap = columnMappingInfo.getDstPkColMap();
            GraphElement dstType = this.graphSchema.getElement(dstLabelId);
            Map<Integer, PropertyValue> dstPkMap = buildPropertiesMap(dstType, items, dstPkColMap);

            BytesRef outEdgeKeyRef =
                    this.dataEncoder.encodeEdgeKey(
                            (GraphVertex) srcType,
                            srcPkMap,
                            (GraphVertex) dstType,
                            dstPkMap,
                            (GraphEdge) type,
                            propertiesMap,
                            tableId,
                            true);
            outKey.set(new Object[] {new String(outEdgeKeyRef.getBytes(), charSet)});
            context.write(outKey, outVal);
            BytesRef inEdgeKeyRef =
                    this.dataEncoder.encodeEdgeKey(
                            (GraphVertex) srcType,
                            srcPkMap,
                            (GraphVertex) dstType,
                            dstPkMap,
                            (GraphEdge) type,
                            propertiesMap,
                            tableId,
                            false);
            outKey.set(new Object[] {new String(inEdgeKeyRef.getBytes(), charSet)});
            context.write(outKey, outVal);
        } else {
            throw new IllegalArgumentException(
                    "invalid label [" + labelId + "], only support VertexType and EdgeType");
        }
    }

    private Map<Integer, PropertyValue> buildPropertiesMap(
            GraphElement typeDef, String[] items, Map<Integer, Integer> columnMapping) {
        Map<Integer, PropertyValue> operationProperties = new HashMap<>(columnMapping.size());
        columnMapping.forEach(
                (colIdx, propertyId) -> {
                    GraphProperty propertyDef = typeDef.getProperty(propertyId);
                    if (propertyDef == null) {
                        throw new PropertyDefNotFoundException(
                                "property ["
                                        + propertyId
                                        + "] not found in ["
                                        + typeDef.getLabel()
                                        + "]");
                    }
                    if (colIdx >= items.length) {
                        throw new IllegalArgumentException(
                                "label ["
                                        + typeDef.getLabel()
                                        + "], invalid mapping ["
                                        + colIdx
                                        + "] -> ["
                                        + propertyId
                                        + "], data ["
                                        + Arrays.toString(items)
                                        + "]");
                    }
                    DataType dataType = propertyDef.getDataType();

                    String val = items[colIdx];
                    PropertyValue propertyValue = new PropertyValue(dataType, val);
                    operationProperties.put(propertyId, propertyValue);
                });
        return operationProperties;
    }
}

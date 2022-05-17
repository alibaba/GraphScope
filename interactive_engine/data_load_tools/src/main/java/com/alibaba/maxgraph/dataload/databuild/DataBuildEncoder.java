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

import com.alibaba.graphscope.groot.schema.*;
import com.alibaba.maxgraph.compiler.api.exception.PropertyDefNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.io.IOException;
import java.util.Iterator;

public class DataBuildEncoder {

    public static final SimpleDateFormat SRC_FMT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final SimpleDateFormat DST_FMT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private static GraphSchema graphSchema;
    private static DataEncoder dataEncoder;
    private static String separator;
    private static Map<String, ColumnMappingInfo> fileToColumnMappingInfo;

    private static ObjectMapper objectMapper;
    private static boolean ldbcCustomize;


    public static String[] encoder(String[] conf, String[] items)
            throws IOException {
        objectMapper = new ObjectMapper();
        String tableName = conf[0];
        String schemaJson = conf[1];
        graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        dataEncoder = new DataEncoder(graphSchema);
        String columnMappingsJson = conf[2];
        fileToColumnMappingInfo =
                objectMapper.readValue(
                        columnMappingsJson,
                        new TypeReference<Map<String, ColumnMappingInfo>>() {});
        ldbcCustomize = "true".equalsIgnoreCase(conf[3]);
        DST_FMT.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));

        ColumnMappingInfo columnMappingInfo = fileToColumnMappingInfo.get(tableName);
        if (columnMappingInfo == null) {
            System.out.println("ignore [" + tableName + "]");
            return null;
        }

        int labelId = columnMappingInfo.getLabelId();
        long tableId = columnMappingInfo.getTableId();
        Map<Integer, Integer> propertiesColumnMapping = columnMappingInfo.getPropertiesColMap();
        GraphElement type = graphSchema.getElement(labelId);
        Map<Integer, PropertyValue> propertiesMap =
                buildPropertiesMap(type, items, propertiesColumnMapping);
        BytesRef valRef = dataEncoder.encodeProperties(labelId, propertiesMap);
        byte[] outVal = getBytes(valRef);
        if (type instanceof GraphVertex) {
            BytesRef keyBytesRef =
                    dataEncoder.encodeVertexKey((GraphVertex) type, propertiesMap, tableId);
            byte[] outKey = getBytes(keyBytesRef);
            String[] kv = new String[2];
            kv[0] = new String(outKey);
            kv[1] = new String(outVal);
            return kv;
        } else if (type instanceof GraphEdge) {
            int srcLabelId = columnMappingInfo.getSrcLabelId();
            Map<Integer, Integer> srcPkColMap = columnMappingInfo.getSrcPkColMap();
            GraphElement srcType = graphSchema.getElement(srcLabelId);
            Map<Integer, PropertyValue> srcPkMap = buildPropertiesMap(srcType, items, srcPkColMap);

            int dstLabelId = columnMappingInfo.getDstLabelId();
            Map<Integer, Integer> dstPkColMap = columnMappingInfo.getDstPkColMap();
            GraphElement dstType = graphSchema.getElement(dstLabelId);
            Map<Integer, PropertyValue> dstPkMap = buildPropertiesMap(dstType, items, dstPkColMap);

            BytesRef outEdgeKeyRef =
                    dataEncoder.encodeEdgeKey(
                            (GraphVertex) srcType,
                            srcPkMap,
                            (GraphVertex) dstType,
                            dstPkMap,
                            (GraphEdge) type,
                            propertiesMap,
                            tableId,
                            true);
            String[] kv = new String[3];
            byte[] outKey = getBytes(outEdgeKeyRef);
            kv[0] = new String(outKey);
            BytesRef inEdgeKeyRef =
                    dataEncoder.encodeEdgeKey(
                            (GraphVertex) srcType,
                            srcPkMap,
                            (GraphVertex) dstType,
                            dstPkMap,
                            (GraphEdge) type,
                            propertiesMap,
                            tableId,
                            false);
            outKey = getBytes(inEdgeKeyRef);
            kv[1] = new String(outKey);
            kv[2] = new String(outVal);
            return kv;
        } else {
            throw new IllegalArgumentException(
                    "invalid label [" + labelId + "], only support VertexType and EdgeType");
        }
    }

    private static byte[] getBytes(BytesRef keyBytesRef) {
        int offset = keyBytesRef.getOffset();
        int length = keyBytesRef.getLength();
        byte[] dst = new byte[length];
        byte[] src = keyBytesRef.getArray();
        System.arraycopy(src, offset, dst, 0, length);
        return dst;
    }

    private static Map<Integer, PropertyValue> buildPropertiesMap(
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
                                        + items
                                        + "]");
                    }
                    DataType dataType = propertyDef.getDataType();

                    String val = items[colIdx];
                    if (ldbcCustomize) {
                        String name = propertyDef.getName();
                        switch (name) {
                            case "creationDate":
                            case "joinDate":
                                val = converteDate(val);
                                break;
                            case "birthday":
                                val = val.replace("-", "");
                                break;
                        }
                    }
                    PropertyValue propertyValue = new PropertyValue(dataType, val);
                    operationProperties.put(propertyId, propertyValue);
                });
        return operationProperties;
    }

    public static String converteDate(String input) {
        try {
            return DST_FMT.format(SRC_FMT.parse(input));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}

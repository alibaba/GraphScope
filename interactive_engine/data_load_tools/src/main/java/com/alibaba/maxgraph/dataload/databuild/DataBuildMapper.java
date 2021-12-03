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

import com.alibaba.maxgraph.compiler.api.exception.PropertyDefNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.graphscope.groot.schema.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataBuildMapper extends Mapper<LongWritable, Text, BytesWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(DataBuildMapper.class);

    public static final SimpleDateFormat SRC_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public static final SimpleDateFormat DST_FMT = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private GraphSchema graphSchema;
    private DataEncoder dataEncoder;
    private String separator;
    private Map<String, ColumnMappingInfo> fileToColumnMappingInfo;

    private ObjectMapper objectMapper;
    private BytesWritable outKey = new BytesWritable();
    private BytesWritable outVal = new BytesWritable();
    private boolean ldbcCustomize;
    private boolean skipHeader;

    @Override
    protected void setup(Context context) throws IOException {
        this.objectMapper = new ObjectMapper();
        Configuration conf = context.getConfiguration();
        this.separator = conf.get(OfflineBuild.SEPARATOR);
        String schemaJson = conf.get(OfflineBuild.SCHEMA_JSON);
        this.graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        this.dataEncoder = new DataEncoder(this.graphSchema);
        String columnMappingsJson = conf.get(OfflineBuild.COLUMN_MAPPINGS);
        this.fileToColumnMappingInfo = this.objectMapper.readValue(columnMappingsJson,
                new TypeReference<Map<String, ColumnMappingInfo>>() {});
        this.ldbcCustomize = conf.getBoolean(OfflineBuild.LDBC_CUSTOMIZE, false);
        this.skipHeader = conf.getBoolean(OfflineBuild.SKIP_HEADER, true);
        DST_FMT.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (skipHeader && key.get() == 0L) {
            return;
        }
        String fullPath = context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
        String fileName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
        ColumnMappingInfo columnMappingInfo = this.fileToColumnMappingInfo.get(fileName);
        if (columnMappingInfo == null) {
            logger.warn("ignore [" + fileName + "]");
            return;
        }

        int labelId = columnMappingInfo.getLabelId();
        long tableId = columnMappingInfo.getTableId();
        Map<Integer, Integer> propertiesColumnMapping = columnMappingInfo.getPropertiesColMap();
        String[] items = value.toString().split(separator);
        GraphElement type = this.graphSchema.getElement(labelId);
        Map<Integer, PropertyValue> propertiesMap = buildPropertiesMap(type, items, propertiesColumnMapping);
        BytesRef valRef = this.dataEncoder.encodeProperties(labelId, propertiesMap);
        this.outVal.set(valRef.getArray(), valRef.getOffset(), valRef.getLength());
        if (type instanceof GraphVertex) {
            BytesRef keyBytesRef = this.dataEncoder.encodeVertexKey((GraphVertex) type, propertiesMap, tableId);
            this.outKey.set(keyBytesRef.getArray(), keyBytesRef.getOffset(), keyBytesRef.getLength());
            context.write(this.outKey, this.outVal);
        } else if (type instanceof GraphEdge) {
            int srcLabelId = columnMappingInfo.getSrcLabelId();
            Map<Integer, Integer> srcPkColMap = columnMappingInfo.getSrcPkColMap();
            GraphElement srcType = this.graphSchema.getElement(srcLabelId);
            Map<Integer, PropertyValue> srcPkMap = buildPropertiesMap(srcType, items, srcPkColMap);

            int dstLabelId = columnMappingInfo.getDstLabelId();
            Map<Integer, Integer> dstPkColMap = columnMappingInfo.getDstPkColMap();
            GraphElement dstType = this.graphSchema.getElement(dstLabelId);
            Map<Integer, PropertyValue> dstPkMap = buildPropertiesMap(dstType, items, dstPkColMap);

            BytesRef outEdgeKeyRef = this.dataEncoder.encodeEdgeKey((GraphVertex) srcType, srcPkMap,
                    (GraphVertex) dstType, dstPkMap, (GraphEdge) type, propertiesMap, tableId, true);
            this.outKey.set(outEdgeKeyRef.getArray(), outEdgeKeyRef.getOffset(), outEdgeKeyRef.getLength());
            context.write(this.outKey, this.outVal);
            BytesRef inEdgeKeyRef = this.dataEncoder.encodeEdgeKey((GraphVertex) srcType, srcPkMap, (GraphVertex) dstType,
                    dstPkMap, (GraphEdge) type, propertiesMap, tableId, false);
            this.outKey.set(inEdgeKeyRef.getArray(), inEdgeKeyRef.getOffset(), inEdgeKeyRef.getLength());
            context.write(this.outKey, this.outVal);
        } else {
            throw new IllegalArgumentException("invalid label [" + labelId + "], only support VertexType and EdgeType");
        }
    }

    private Map<Integer, PropertyValue> buildPropertiesMap(GraphElement typeDef, String[] items,
                                                           Map<Integer, Integer> columnMapping) {
        Map<Integer, PropertyValue> operationProperties = new HashMap<>(columnMapping.size());
        columnMapping.forEach((colIdx, propertyId) -> {
            GraphProperty propertyDef = typeDef.getProperty(propertyId);
            if (propertyDef == null) {
                throw new PropertyDefNotFoundException("property [" + propertyId + "] not found in [" +
                        typeDef.getLabel() + "]");
            }
            if (colIdx >= items.length) {
                throw new IllegalArgumentException("label [" + typeDef.getLabel() + "], invalid mapping [" + colIdx +
                        "] -> [" + propertyId + "], data [" + items + "]");
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

/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.groot.common.config.DataLoadConfig;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.schema.mapper.GraphSchemaMapper;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
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
import java.util.*;

public class DataBuildMapper extends Mapper<LongWritable, Text, BytesWritable, BytesWritable> {
    private static final Logger logger = LoggerFactory.getLogger(DataBuildMapper.class);

    private GraphSchema graphSchema;
    private DataEncoder dataEncoder;
    private String separator;
    private Map<String, ColumnMappingInfo> fileToColumnMappingInfo;

    private final BytesWritable outKey = new BytesWritable();
    private final BytesWritable outVal = new BytesWritable();
    private boolean skipHeader;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        this.separator = conf.get(DataLoadConfig.SEPARATOR);
        String schemaJson = conf.get(DataLoadConfig.SCHEMA_JSON);
        this.graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        this.dataEncoder = new DataEncoder(this.graphSchema);
        String columnMappingsJson = conf.get(DataLoadConfig.COLUMN_MAPPINGS);
        ObjectMapper objectMapper = new ObjectMapper();
        this.fileToColumnMappingInfo =
                objectMapper.readValue(
                        columnMappingsJson, new TypeReference<Map<String, ColumnMappingInfo>>() {});
        this.skipHeader = conf.getBoolean(DataLoadConfig.SKIP_HEADER, true);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (skipHeader && key.get() == 0L) {
            return;
        }
        String fullPath = context.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE);
        String fileName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
        ColumnMappingInfo info = this.fileToColumnMappingInfo.get(fileName);
        if (info == null) {
            logger.warn("Mapper: ignore [{}], fullPath is [{}]", fileName, fullPath);
            return;
        }

        String[] items = value.toString().split(separator);

        int labelId = info.getLabelId();
        long tableId = info.getTableId();
        GraphElement type = this.graphSchema.getElement(labelId);
        Map<Integer, Integer> colMap = info.getPropertiesColMap();
        Map<Integer, PropertyValue> properties = Utils.buildProperties(type, items, colMap);

        BytesRef valRef = dataEncoder.encodeProperties(labelId, properties);
        this.outVal.set(valRef.getArray(), valRef.getOffset(), valRef.getLength());

        if (type instanceof GraphVertex) {
            BytesRef keyRef =
                    Utils.getVertexKeyRef(dataEncoder, (GraphVertex) type, properties, tableId);
            this.outKey.set(keyRef.getArray(), keyRef.getOffset(), keyRef.getLength());
            context.write(this.outKey, this.outVal);
        } else if (type instanceof GraphEdge) {
            BytesRef out =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, true);
            this.outKey.set(out.getArray(), out.getOffset(), out.getLength());
            context.write(this.outKey, this.outVal);
            BytesRef in =
                    Utils.getEdgeKeyRef(
                            dataEncoder, graphSchema, info, items, properties, tableId, false);
            this.outKey.set(in.getArray(), in.getOffset(), in.getLength());
            context.write(this.outKey, this.outVal);
        } else {
            throw new InvalidArgumentException("Invalid label " + labelId);
        }
    }
}

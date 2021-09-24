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

import com.alibaba.maxgraph.compiler.api.exception.InvalidSchemaException;
import com.alibaba.maxgraph.compiler.api.schema.*;

import java.util.HashMap;
import java.util.Map;

public class FileColumnMapping {
    private String label;
    private String srcLabel;
    private String dstLabel;

    private Map<Integer, String> srcPkColMap;
    private Map<Integer, String> dstPkColMap;
    private Map<Integer, String> propertiesColMap;

    public FileColumnMapping() {

    }

    public FileColumnMapping(String label, Map<Integer, String> propertiesColMap) {
        this(label, null, null, null, null, propertiesColMap);
    }

    public FileColumnMapping(String label, String srcLabel, String dstLabel, Map<Integer, String> srcPkColMap,
                             Map<Integer, String> dstPkColMap, Map<Integer, String> propertiesColMap) {
        this.label = label;
        this.srcLabel = srcLabel;
        this.dstLabel = dstLabel;
        this.srcPkColMap = srcPkColMap;
        this.dstPkColMap = dstPkColMap;
        this.propertiesColMap = propertiesColMap;
    }

    public String getLabel() {
        return label;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    public Map<Integer, String> getSrcPkColMap() {
        return srcPkColMap;
    }

    public Map<Integer, String> getDstPkColMap() {
        return dstPkColMap;
    }

    public Map<Integer, String> getPropertiesColMap() {
        return propertiesColMap;
    }

    public ColumnMappingInfo toColumnMappingInfo(GraphSchema graphSchema) {
        GraphElement type = graphSchema.getElement(this.label);
        int labelId = type.getLabelId();
        Map<Integer, Integer> propertiesMap = convertMapValueToId(this.propertiesColMap, graphSchema);
        if (type instanceof GraphVertex) {
            long tableId = ((GraphVertex) type).getTableId();
            return new ColumnMappingInfo(labelId, tableId, propertiesMap);
        } else {
            GraphElement srcType = graphSchema.getElement(this.srcLabel);
            int srcLabelId = srcType.getLabelId();
            Map<Integer, Integer> srcPkMap = convertMapValueToId(this.srcPkColMap, graphSchema);
            GraphElement dstType = graphSchema.getElement(this.dstLabel);
            int dstLabelId = dstType.getLabelId();
            Map<Integer, Integer> dstPkMap = convertMapValueToId(this.dstPkColMap, graphSchema);
            for (EdgeRelation relation : ((GraphEdge) type).getRelationList()) {
                if (relation.getSource().getLabelId() == srcLabelId &&
                        relation.getTarget().getLabelId() == dstLabelId) {
                    long tableId = relation.getTableId();
                    return new ColumnMappingInfo(labelId, srcLabelId, dstLabelId, tableId, srcPkMap, dstPkMap,
                            propertiesMap);
                }
            }
            throw new InvalidSchemaException("invalid mapping for label [" + this.label + "] srcLabel [" + this.srcLabel
                    + "] dstLabel [" + this.dstLabel + "]");
        }
    }

    private Map<Integer, Integer> convertMapValueToId(Map<Integer, String> colNameMap, GraphSchema graphSchema) {
        Map<Integer, Integer> res = new HashMap<>(colNameMap.size());
        colNameMap.forEach((colIdx, propName) -> {
            int propertyId = graphSchema.getPropertyId(propName);
            res.put(colIdx, propertyId);
        });
        return res;
    }




}

package com.alibaba.maxgraph.databuild;

import com.alibaba.maxgraph.v2.common.exception.InvalidSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.*;
import com.fasterxml.jackson.annotation.JsonCreator;

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
        SchemaElement type = graphSchema.getSchemaElement(this.label);
        int labelId = type.getLabelId();
        Map<Integer, Integer> propertiesMap = convertMapValueToId(this.propertiesColMap, graphSchema);
        if (type instanceof VertexType) {
            long tableId = ((VertexType) type).getTableId();
            return new ColumnMappingInfo(labelId, tableId, propertiesMap);
        } else {
            SchemaElement srcType = graphSchema.getSchemaElement(this.srcLabel);
            int srcLabelId = srcType.getLabelId();
            Map<Integer, Integer> srcPkMap = convertMapValueToId(this.srcPkColMap, graphSchema);
            SchemaElement dstType = graphSchema.getSchemaElement(this.dstLabel);
            int dstLabelId = dstType.getLabelId();
            Map<Integer, Integer> dstPkMap = convertMapValueToId(this.dstPkColMap, graphSchema);
            for (EdgeRelation relation : ((EdgeType) type).getRelationList()) {
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
            int propertyId = graphSchema.getPropertyId(propName).values().iterator().next();
            res.put(colIdx, propertyId);
        });
        return res;
    }




}

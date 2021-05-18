package com.alibaba.maxgraph.dataload.databuild;

import java.util.Map;

public class ColumnMappingInfo {

    private int labelId;
    private int srcLabelId;
    private int dstLabelId;
    private long tableId;

    private Map<Integer, Integer> srcPkColMap;
    private Map<Integer, Integer> dstPkColMap;
    private Map<Integer, Integer> propertiesColMap;

    public ColumnMappingInfo() {}

    public ColumnMappingInfo(int labelId, long tableId, Map<Integer, Integer> propertiesColMap) {
        this(labelId, -1, -1, tableId, null, null, propertiesColMap);
    }

    public ColumnMappingInfo(int labelId, int srcLabelId, int dstLabelId, long tableId,
                             Map<Integer, Integer> srcPkColMap, Map<Integer, Integer> dstPkColMap,
                             Map<Integer, Integer> propertiesColMap) {
        this.labelId = labelId;
        this.srcLabelId = srcLabelId;
        this.dstLabelId = dstLabelId;
        this.tableId = tableId;
        this.srcPkColMap = srcPkColMap;
        this.dstPkColMap = dstPkColMap;
        this.propertiesColMap = propertiesColMap;
    }

    public long getTableId() {
        return tableId;
    }

    public int getLabelId() {
        return labelId;
    }

    public int getSrcLabelId() {
        return srcLabelId;
    }

    public int getDstLabelId() {
        return dstLabelId;
    }

    public Map<Integer, Integer> getSrcPkColMap() {
        return srcPkColMap;
    }

    public Map<Integer, Integer> getDstPkColMap() {
        return dstPkColMap;
    }

    public Map<Integer, Integer> getPropertiesColMap() {
        return propertiesColMap;
    }
}

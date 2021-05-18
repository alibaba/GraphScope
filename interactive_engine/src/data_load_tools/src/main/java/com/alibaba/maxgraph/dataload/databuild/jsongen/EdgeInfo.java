package com.alibaba.maxgraph.dataload.databuild.jsongen;

import com.alibaba.maxgraph.dataload.databuild.FileColumnMapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class EdgeInfo {
    abstract String getLabel();
    abstract String getSrcLabel();
    abstract String getDstLabel();
    abstract String[] getPropertyNames();

    public FileColumnMapping toFileColumnMapping() {
        String label = getLabel();
        String srcLabel = getSrcLabel();
        String dstLabel = getDstLabel();
        String[] propertyNames = getPropertyNames();
        Map<Integer, String> colMapping = new HashMap<>();
        for (int i = 0; i < propertyNames.length; i++) {
            colMapping.put(i + 2, propertyNames[i]);
        }
        Map<Integer, String> srcColMapping = Collections.singletonMap(0, "id");
        Map<Integer, String> dstColMapping = Collections.singletonMap(1, "id");
        return new FileColumnMapping(label, srcLabel, dstLabel, srcColMapping, dstColMapping, colMapping);
    }
}

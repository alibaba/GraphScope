package com.alibaba.maxgraph.databuild.jsongen;

import com.alibaba.maxgraph.databuild.FileColumnMapping;

import java.util.HashMap;
import java.util.Map;

public abstract class VertexInfo {
    abstract String getLabel();
    abstract String[] getPropertyNames();

    public FileColumnMapping toFileColumnMapping() {
        String label = getLabel();
        String[] propertyNames = getPropertyNames();
        Map<Integer, String> colMapping = new HashMap<>();
        for (int i = 0; i < propertyNames.length; i++) {
            colMapping.put(i, propertyNames[i]);
        }
        return new FileColumnMapping(label, colMapping);
    }
}

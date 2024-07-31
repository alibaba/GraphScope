package com.alibaba.graphscope.groot.dataload.unified;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class VertexMapping {
    public String typeName;
    public List<String> inputs;
    public List<ColumnMapping> columnMappings;

    @JsonIgnore
    public String getInputFileName() {
        String input = inputs.get(0);
        String[] parts = input.trim().split("/");
        return parts[parts.length - 1];
    }
}

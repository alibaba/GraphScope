package com.alibaba.graphscope.groot.dataload.unified;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class EdgeMapping {
    public TypeTriplet typeTriplet;
    public List<String> inputs;
    public List<ColumnMapping> columnMappings;

    public List<ColumnMapping> sourceVertexMappings;
    public List<ColumnMapping> destinationVertexMappings;

    @JsonIgnore
    public String getInputFileName() {
        String input = inputs.get(0);
        String[] parts = input.trim().split("/");
        return parts[parts.length - 1];
    }
}

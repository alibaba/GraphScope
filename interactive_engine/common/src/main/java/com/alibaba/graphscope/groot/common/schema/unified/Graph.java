package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.exception.PropertyNotFoundException;
import com.alibaba.graphscope.groot.common.exception.TypeNotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonDeserialize(converter = Converter.class)
public class Graph implements GraphSchema {
    public String name;
    public String version;
    public Schema schema;

    public static Graph parseFromYaml(String yaml) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.findAndRegisterModules();
        Graph graph = mapper.readValue(yaml, Graph.class);
        return graph;
    }

    @Override
    public String toString() {
        return "Graph{" + "name='" + name + '\'' + ", schema=" + schema + '}';
    }

    @Override
    public GraphElement getElement(String label) throws TypeNotFoundException {
        for (Type type : schema.vertexTypes) {
            if (Objects.equals(type.getLabel(), label)) {
                return type;
            }
        }
        for (Type type : schema.edgeTypes) {
            if (Objects.equals(type.getLabel(), label)) {
                return type;
            }
        }
        return null;
    }

    @Override
    public GraphElement getElement(int labelId) throws TypeNotFoundException {
        for (Type type : schema.vertexTypes) {
            if (type.getLabelId() == labelId) {
                return type;
            }
        }
        for (Type type : schema.edgeTypes) {
            if (type.getLabelId() == labelId) {
                return type;
            }
        }
        return null;
    }

    @Override
    @JsonIgnore
    public List<GraphVertex> getVertexList() {
        return new ArrayList<>(schema.vertexTypes);
    }

    @Override
    @JsonIgnore
    public List<GraphEdge> getEdgeList() {
        return new ArrayList<>(schema.edgeTypes);
    }

    @Override
    public Integer getPropertyId(String propName) throws PropertyNotFoundException {
        for (Type type : schema.vertexTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getName(), propName)) {
                    return property.getId();
                }
            }
        }
        for (Type type : schema.edgeTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getName(), propName)) {
                    return property.getId();
                }
            }
        }
        return null;
    }

    @Override
    public String getPropertyName(int propId) throws PropertyNotFoundException {
        for (Type type : schema.vertexTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getId(), propId)) {
                    return property.getName();
                }
            }
        }
        for (Type type : schema.edgeTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getId(), propId)) {
                    return property.getName();
                }
            }
        }
        return null;
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(String propName) {
        Map<GraphElement, GraphProperty> elementPropertyList = Maps.newHashMap();
        for (Type type : schema.vertexTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getName(), propName)) {
                    elementPropertyList.put(type, property);
                }
            }
        }
        for (Type type : schema.edgeTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getName(), propName)) {
                    elementPropertyList.put(type, property);
                }
            }
        }
        return elementPropertyList;
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(int propId) {
        Map<GraphElement, GraphProperty> elementPropertyList = Maps.newHashMap();
        for (Type type : schema.vertexTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getId(), propId)) {
                    elementPropertyList.put(type, property);
                }
            }
        }
        for (Type type : schema.edgeTypes) {
            for (GraphProperty property : type.getPropertyList()) {
                if (Objects.equals(property.getId(), propId)) {
                    elementPropertyList.put(type, property);
                }
            }
        }
        return elementPropertyList;
    }

    @Override
    @JsonIgnore
    public String getVersion() {
        if (version == null) {
            return "0";
        }
        return version;
    }
}

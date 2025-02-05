/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.util.IrSchemaParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaSpecManager {
    private static final Logger logger = LoggerFactory.getLogger(SchemaSpecManager.class);
    private final GraphSchema rootSchema;
    private final boolean isColumnId;
    private final RelDataTypeFactory typeFactory;
    private final List<SchemaSpec> specifications;

    public SchemaSpecManager(
            GraphSchema rootSchema, boolean isColumnId, RelDataTypeFactory typeFactory) {
        this.rootSchema = rootSchema;
        this.isColumnId = isColumnId;
        this.typeFactory = typeFactory;
        this.specifications = Lists.newArrayList();
    }

    public SchemaSpecManager(
            GraphSchema rootSchema,
            boolean isColumnId,
            RelDataTypeFactory typeFactory,
            SchemaSpec input) {
        this.rootSchema = rootSchema;
        this.isColumnId = isColumnId;
        this.typeFactory = typeFactory;
        this.specifications = Lists.newArrayList(input);
    }

    public SchemaSpec getSpec(SchemaSpec.Type type) {
        for (SchemaSpec spec : specifications) {
            if (spec.getType() == type) {
                return spec;
            }
        }
        SchemaSpec newSpec;
        switch (type) {
            case IR_CORE_IN_JSON:
                newSpec =
                        new SchemaSpec(
                                type, IrSchemaParser.getInstance().parse(rootSchema, isColumnId));
                break;
            case FLEX_IN_JSON:
                SchemaSpec yamlSpec = getSpec(SchemaSpec.Type.FLEX_IN_YAML);
                Preconditions.checkArgument(
                        yamlSpec != null,
                        "cannot get schema specification of type " + SchemaSpec.Type.FLEX_IN_YAML);
                Yaml yaml = new Yaml();
                Map yamlMap = yaml.load(yamlSpec.getContent());
                ObjectMapper mapper = new ObjectMapper();
                try {
                    newSpec = new SchemaSpec(type, mapper.writeValueAsString(yamlMap));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                break;
            case FLEX_IN_YAML:
            default:
                newSpec = convertToFlex(rootSchema);
                break;
        }
        this.specifications.add(newSpec);
        return newSpec;
    }

    private SchemaSpec convertToFlex(GraphSchema schema) {
        List<Map> vertices =
                schema.getVertexList().stream()
                        .map(this::convertVertex)
                        .collect(Collectors.toList());
        List<Map> edges =
                schema.getEdgeList().stream().map(this::convertEdge).collect(Collectors.toList());
        Map<String, Object> flexMap =
                ImmutableMap.of(
                        "schema", ImmutableMap.of("vertex_types", vertices, "edge_types", edges));
        Yaml yaml = new Yaml();
        return new SchemaSpec(SchemaSpec.Type.FLEX_IN_YAML, yaml.dump(flexMap));
    }

    private Map<String, Object> convertVertex(GraphVertex vertex) {
        return ImmutableMap.of(
                "type_name", vertex.getLabel(),
                "type_id", vertex.getLabelId(),
                "properties",
                        vertex.getPropertyList().stream()
                                .map(this::convertProperty)
                                .collect(Collectors.toList()),
                "primary_keys",
                        vertex.getPrimaryKeyList().stream()
                                .map(GraphProperty::getName)
                                .collect(Collectors.toList()));
    }

    private Map<String, Object> convertEdge(GraphEdge edge) {
        return ImmutableMap.of(
                "type_name", edge.getLabel(),
                "type_id", edge.getLabelId(),
                "vertex_type_pair_relations",
                        edge.getRelationList().stream()
                                .map(this::convertRelation)
                                .collect(Collectors.toList()),
                "properties",
                        edge.getPropertyList().stream()
                                .map(this::convertProperty)
                                .collect(Collectors.toList()),
                "primary_keys",
                        edge.getPrimaryKeyList().stream()
                                .map(GraphProperty::getName)
                                .collect(Collectors.toList()));
    }

    private Map<String, Object> convertRelation(EdgeRelation relation) {
        return ImmutableMap.of(
                "source_vertex", relation.getSource().getLabel(),
                "destination_vertex", relation.getTarget().getLabel());
    }

    private Map<String, Object> convertProperty(GraphProperty property) {
        RelDataType propertyType;
        if (property instanceof IrGraphProperty) {
            propertyType = ((IrGraphProperty) property).getRelDataType();
        } else {
            propertyType =
                    (new IrDataTypeConvertor.Groot(typeFactory, false))
                            .convert(property.getDataType());
        }
        // convert property type to flex format
        IrDataTypeConvertor.Flex flexConvertor = new IrDataTypeConvertor.Flex(typeFactory, false);
        GSDataTypeDesc typeDesc = flexConvertor.convert(propertyType);
        return ImmutableMap.of(
                "property_id", property.getId(),
                "property_name", property.getName(),
                "property_type", typeDesc.getYamlDesc());
    }
}

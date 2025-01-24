/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpec;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpecManager;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.*;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

// test schema conversion among different specifications
public class SchemaSpecTest {
    @Test
    public void testGrootSpecConversion() throws Exception {
        GraphSchema grootSchema = mockGrootSchema();
        RelDataTypeFactory typeFactory = new GraphTypeFactoryImpl(new Configs(ImmutableMap.of()));
        SchemaSpecManager specManager = new SchemaSpecManager(grootSchema, true, typeFactory);
        // convert graph schema to different specifications
        SchemaSpec flexYamlSpec = specManager.getSpec(SchemaSpec.Type.FLEX_IN_YAML);
        SchemaSpec flexJsonSpec = specManager.getSpec(SchemaSpec.Type.FLEX_IN_JSON);
        SchemaSpec irCoreJsonSpec = specManager.getSpec(SchemaSpec.Type.IR_CORE_IN_JSON);
        // convert specifications back to graph schema
        GraphSchema schema1 = flexYamlSpec.convert(typeFactory);
        GraphSchema schema2 = flexJsonSpec.convert(typeFactory);
        GraphSchema schema3 = irCoreJsonSpec.convert(typeFactory);
        Assert.assertEquals(schema1.getVertexList(), schema2.getVertexList());
        Assert.assertEquals(schema1.getEdgeList(), schema2.getEdgeList());
        Assert.assertEquals(schema2.getVertexList(), schema3.getVertexList());
        Assert.assertEquals(schema2.getEdgeList(), schema3.getEdgeList());
    }

    private GraphSchema mockGrootSchema() {
        GraphVertex personVertex =
                new DefaultGraphVertex(
                        0,
                        "PERSON",
                        ImmutableList.of(
                                new DefaultGraphProperty(0, "id", DataType.INT),
                                new DefaultGraphProperty(1, "name", DataType.STRING)),
                        ImmutableList.of("id"));
        Map<String, GraphVertex> vertexMap = ImmutableMap.of("PERSON", personVertex);
        Map<String, GraphEdge> edgeMap =
                ImmutableMap.of(
                        "KNOWS",
                        new DefaultGraphEdge(
                                0,
                                "KNOWS",
                                ImmutableList.of(
                                        new DefaultGraphProperty(0, "id", DataType.INT),
                                        new DefaultGraphProperty(1, "weight", DataType.DOUBLE)),
                                ImmutableList.of(
                                        new DefaultEdgeRelation(personVertex, personVertex))));
        GraphSchema schema =
                new DefaultGraphSchema(
                        vertexMap, edgeMap, ImmutableMap.of("id", 0, "name", 1, "weight", 1));
        return schema;
    }
}

/*
 * Copyright 2024 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.rel.metadata;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class GLogueSchemaTest {
    @Test
    public void glogue_schema_test() {
        GlogueSchema g = mockGlogueSchema();
        Double delta = 0.00001;

        Integer person = 0;
        Integer software = 1;
        EdgeTypeId knows = new EdgeTypeId(0, 0, 0);
        EdgeTypeId creates = new EdgeTypeId(0, 1, 1);

        Assert.assertEquals(2, g.getVertexTypes().size());
        Assert.assertEquals(2, g.getEdgeTypes().size());

        // test get type cardinality
        Assert.assertEquals(4.0, g.getVertexTypeCardinality(person), delta);
        Assert.assertEquals(2.0, g.getVertexTypeCardinality(software), delta);
        Assert.assertEquals(2.0, g.getEdgeTypeCardinality(knows), delta);
        Assert.assertEquals(4.0, g.getEdgeTypeCardinality(creates), delta);

        // when query a type that is not in the schema, return 1.0
        Assert.assertEquals(1.0, g.getVertexTypeCardinality(2), delta);
        Assert.assertEquals(1.0, g.getEdgeTypeCardinality(new EdgeTypeId(0, 0, 2)), delta);

        // test get types
        Assert.assertEquals(2, g.getAdjEdgeTypes(person).size());
        Assert.assertEquals(1, g.getAdjEdgeTypes(software).size());
        Assert.assertEquals(1, g.getEdgeTypes(person, person).size());
        Assert.assertEquals(1, g.getEdgeTypes(person, software).size());
        Assert.assertEquals(0, g.getEdgeTypes(software, person).size());
    }

    static GlogueSchema mockGlogueSchema() {
        // modern graph schema
        // person: label 0, statistics 4;
        // software: label 1, statistics 2;
        // person-knows->person: label 0, statistics 2;
        // person-created->software: label 1, statistics 4;
        GraphSchema graphSchema = Utils.schemaMeta.getSchema();
        graphSchema.getElement("person").getLabelId();
        Integer person = graphSchema.getElement("person").getLabelId();
        Integer software = graphSchema.getElement("software").getLabelId();
        EdgeTypeId knows = new EdgeTypeId(0, 0, graphSchema.getElement("knows").getLabelId());
        EdgeTypeId creates = new EdgeTypeId(0, 1, graphSchema.getElement("created").getLabelId());
        HashMap<Integer, Double> vertexTypeCardinality = new HashMap<Integer, Double>();
        vertexTypeCardinality.put(person, 4.0);
        vertexTypeCardinality.put(software, 2.0);
        HashMap<EdgeTypeId, Double> edgeTypeCardinality = new HashMap<EdgeTypeId, Double>();
        edgeTypeCardinality.put(knows, 2.0);
        edgeTypeCardinality.put(creates, 4.0);

        GlogueSchema g = new GlogueSchema(graphSchema, vertexTypeCardinality, edgeTypeCardinality);
        return g;
    }
}

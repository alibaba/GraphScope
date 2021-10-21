/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.cost;

import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.cost.statistics.CostDataStatistics;
import com.alibaba.maxgraph.compiler.cost.statistics.NodeStatistics;
import com.alibaba.maxgraph.compiler.schema.DefaultEdgeRelation;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphEdge;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphProperty;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphSchema;
import com.alibaba.maxgraph.compiler.schema.DefaultGraphVertex;

import com.alibaba.maxgraph.compiler.schema.DefaultSchemaFetcher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CostDataStatisticsTest {
    private CostDataStatistics statistics;
    private GraphSchema schema;

    public CostDataStatisticsTest() {
        DefaultGraphProperty postIdProp = new DefaultGraphProperty(1, "id", DataType.LONG);
        Map<String, Integer> propNameToIdList = Maps.newHashMap();
        propNameToIdList.put("name", 1);

        DefaultGraphVertex post = new DefaultGraphVertex(1, "post", Lists.newArrayList(postIdProp), Lists.newArrayList(postIdProp));
        DefaultGraphVertex comment = new DefaultGraphVertex(2, "comment", Lists.newArrayList(postIdProp), Lists.newArrayList(postIdProp));
        DefaultGraphVertex person = new DefaultGraphVertex(3, "person", Lists.newArrayList(postIdProp), Lists.newArrayList(postIdProp));
        Map<String, GraphVertex> vertexList = Maps.newHashMap();
        vertexList.put(post.getLabel(), post);
        vertexList.put(comment.getLabel(), comment);
        vertexList.put(person.getLabel(), person);

        DefaultGraphEdge create = new DefaultGraphEdge(4, "create", Lists.newArrayList(), Lists.newArrayList(
                new DefaultEdgeRelation(person, post),
                new DefaultEdgeRelation(person, comment)
        ));
        DefaultGraphEdge reply = new DefaultGraphEdge(5, "reply", Lists.newArrayList(), Lists.newArrayList(
                new DefaultEdgeRelation(comment, post),
                new DefaultEdgeRelation(comment, comment)
        ));
        DefaultGraphEdge knows = new DefaultGraphEdge(6, "knows", Lists.newArrayList(), Lists.newArrayList(
                new DefaultEdgeRelation(person, person)
        ));
        Map<String, GraphEdge> edgeList = Maps.newHashMap();
        edgeList.put(create.getLabel(), create);
        edgeList.put(reply.getLabel(), reply);
        edgeList.put(knows.getLabel(), knows);

        schema = new DefaultGraphSchema(vertexList, edgeList, propNameToIdList);
        CostDataStatistics.initialize(new DefaultSchemaFetcher(schema));
        statistics = CostDataStatistics.getInstance();

        statistics.addVertexCount("person", 10000);
        statistics.addVertexCount("post", 1000000);
        statistics.addEdgeCount("comment", 10000000);

        statistics.addEdgeCount("create", 1000000 + 10000000);
        statistics.addEdgeCount("knows", 1000000);
        statistics.addEdgeCount("reply", 4000000);
    }

    @Test
    public void testEmptyRatio() {
        NodeStatistics startStatistics = new NodeStatistics(schema);
        startStatistics.addVertexCount("person", 10);
        startStatistics.addVertexCount("post", 100);

        NodeStatistics outRatio = statistics.getOutRatio(startStatistics, Sets.newHashSet());
        Map<String, Double> outVertexCountList = outRatio.getVertexCountList();
        Assert.assertEquals((Double) 5500.0, outVertexCountList.get("post"));
        Assert.assertEquals((Double) 1000.0, outVertexCountList.get("person"));
        Assert.assertEquals((Double) 5500.0, outVertexCountList.get("comment"));

        NodeStatistics inRatio = statistics.getInRatio(startStatistics, Sets.newHashSet());
        Map<String, Double> inVertexCountList = inRatio.getVertexCountList();
        Assert.assertEquals((Double) 1550.0, inVertexCountList.get("person"));
        Assert.assertEquals((Double) 200.0, inVertexCountList.get("comment"));

        NodeStatistics bothRatio = statistics.getBothRatio(startStatistics, Sets.newHashSet());
        Map<String, Double> bothVertexCountList = bothRatio.getVertexCountList();
        Assert.assertEquals((Double) 5500.0, outVertexCountList.get("post"));
        Assert.assertEquals((Double) 2550.0, bothVertexCountList.get("person"));
        Assert.assertEquals((Double) 5700.0, bothVertexCountList.get("comment"));
    }

    @Test
    public void testEdgeRatio() {
        NodeStatistics startStatistics = new NodeStatistics(schema);
        startStatistics.addVertexCount("person", 10);
        startStatistics.addVertexCount("post", 100);

        NodeStatistics outRatio = statistics.getOutRatio(startStatistics, Sets.newHashSet("knows"));
        Map<String, Double> outVertexCountList = outRatio.getVertexCountList();
        Assert.assertEquals((Double) 1000.0, outVertexCountList.get("person"));

        NodeStatistics inRatio = statistics.getInRatio(startStatistics, Sets.newHashSet("reply", "create"));
        Map<String, Double> inVertexCountList = inRatio.getVertexCountList();
        Assert.assertEquals((Double) 550.0, inVertexCountList.get("person"));
        Assert.assertEquals((Double) 200.0, inVertexCountList.get("comment"));

        NodeStatistics bothRatio = statistics.getBothRatio(startStatistics, Sets.newHashSet("reply"));
        Map<String, Double> bothVertexCountList = bothRatio.getVertexCountList();
        Assert.assertEquals((Double) 200.0, bothVertexCountList.get("comment"));
    }
}

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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendStep;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class GlogueTest {

    static GlogueSchema g = GLogueSchemaTest.mockGlogueSchema();
    static Glogue gl = new Glogue(g, 3);
    static Double delta = 0.00001;
    static Integer person = 0;
    static Integer software = 1;
    static EdgeTypeId knows = new EdgeTypeId(0, 0, 0);
    static EdgeTypeId creates = new EdgeTypeId(0, 1, 1);

    @Test
    public void glogue_get_row_count_test() {

        // p1 -> s0 <- p2 + p1 -> p2
        // v0: software, v1: person1, v2: person2
        PatternVertex v0 = new SinglePatternVertex(software, 0);
        PatternVertex v1 = new SinglePatternVertex(person, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // pattern1: person2 <- person1 -> software
        Pattern pattern1 = new Pattern();
        pattern1.addVertex(v0);
        pattern1.addVertex(v1);
        pattern1.addVertex(v2);
        pattern1.addEdge(v1, v0, creates);
        pattern1.addEdge(v1, v2, knows);
        pattern1.reordering();

        Double count1 = gl.getRowCount(pattern1);
        Assert.assertEquals(2.0d, count1, delta);

        // pattern2: person2 <- person1 -> software + person2 -> software
        Pattern pattern2 = new Pattern();
        pattern2.addVertex(v0);
        pattern2.addVertex(v1);
        pattern2.addVertex(v2);
        pattern2.addEdge(v1, v0, creates);
        pattern2.addEdge(v1, v2, knows);
        pattern2.addEdge(v2, v0, creates);
        pattern2.reordering();

        Double count2 = gl.getRowCount(pattern2);
        Assert.assertEquals(1.0d, count2, delta);

        // pattern3: person2 <- person1 <- software (not exist)
        // Note the non-exist pattern will return a cardinality of 1.0
        Pattern pattern3 = new Pattern();
        pattern3.addVertex(v0);
        pattern3.addVertex(v1);
        pattern3.addVertex(v2);
        pattern3.addEdge(v0, v1, creates);
        pattern3.addEdge(v1, v2, knows);
        pattern3.reordering();

        Double count3 = gl.getRowCount(pattern3);
        Assert.assertEquals(1.0d, count3, delta);
    }

    @Test
    public void glogue_get_neighbors_test() {
        // v0: software, v1: person1, v2: person2
        PatternVertex v0 = new SinglePatternVertex(software, 0);
        PatternVertex v1 = new SinglePatternVertex(person, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // pattern1: person2 <- person1 -> software
        Pattern pattern1 = new Pattern();
        pattern1.addVertex(v0);
        pattern1.addVertex(v1);
        pattern1.addVertex(v2);
        pattern1.addEdge(v1, v0, creates);
        pattern1.addEdge(v1, v2, knows);
        pattern1.reordering();

        Set<GlogueEdge> outEdges = gl.getOutEdges(pattern1);
        Set<GlogueEdge> inEdges = gl.getInEdges(pattern1);
        Assert.assertEquals(0, outEdges.size());
        Assert.assertEquals(2, inEdges.size());

        // pattern2: person2 <- person1 -> software + person2 -> software
        Pattern pattern2 = new Pattern();
        pattern2.addVertex(v0);
        pattern2.addVertex(v1);
        pattern2.addVertex(v2);
        pattern2.addEdge(v1, v0, creates);
        pattern2.addEdge(v1, v2, knows);
        pattern2.addEdge(v2, v0, creates);
        pattern2.reordering();

        outEdges = gl.getOutEdges(pattern2);
        inEdges = gl.getInEdges(pattern2);
        Assert.assertEquals(0, outEdges.size());
        Assert.assertEquals(2, inEdges.size());

        // pattern3: person1 -> person2
        Pattern pattern3 = new Pattern();
        pattern3.addVertex(v1);
        pattern3.addVertex(v2);
        pattern3.addEdge(v1, v2, knows);
        pattern3.reordering();

        outEdges = gl.getOutEdges(pattern3);
        inEdges = gl.getInEdges(pattern3);
        Assert.assertEquals(14, outEdges.size());
        Assert.assertEquals(1, inEdges.size());

        // pattern4: person1 -> software
        Pattern pattern4 = new Pattern();
        pattern4.addVertex(v0);
        pattern4.addVertex(v1);
        pattern4.addEdge(v1, v0, creates);
        pattern4.reordering();

        outEdges = gl.getOutEdges(pattern4);
        inEdges = gl.getInEdges(pattern4);
        Assert.assertEquals(7, outEdges.size());
        Assert.assertEquals(2, inEdges.size());
    }

    @Test
    public void glogue_edge_weight_test() {
        // v0: software, v1: person1, v2: person2
        PatternVertex v0 = new SinglePatternVertex(software, 0);
        PatternVertex v1 = new SinglePatternVertex(person, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // pattern1: person2 <- person1 -> software
        Pattern pattern1 = new Pattern();
        pattern1.addVertex(v0);
        pattern1.addVertex(v1);
        pattern1.addVertex(v2);
        pattern1.addEdge(v1, v0, creates);
        pattern1.addEdge(v1, v2, knows);
        pattern1.reordering();

        Set<GlogueEdge> outEdges = gl.getOutEdges(pattern1);
        Set<GlogueEdge> inEdges = gl.getInEdges(pattern1);
        Assert.assertEquals(0, outEdges.size());
        Assert.assertEquals(2, inEdges.size());

        for (GlogueEdge edge : inEdges) {

            GlogueExtendIntersectEdge e = (GlogueExtendIntersectEdge) edge;
            Assert.assertEquals(2, e.getSrcPattern().getVertexSet().size());
            ExtendStep step = e.getExtendStep();
            if (step.getTargetVertexType() == software) {
                // person2<-person1 + extend: person1->software
                Assert.assertEquals(1, step.getExtendEdges().size());
                Assert.assertEquals(1.0, step.getWeight().doubleValue(), delta);
            } else if (step.getTargetVertexType() == person) {
                // person1->software + extend: person1->person2
                Assert.assertEquals(1, step.getExtendEdges().size());
                Assert.assertEquals(0.5, step.getWeight().doubleValue(), delta);
            }
        }

        // pattern1: person2 <- person1 -> software + person2->software
        Pattern pattern2 = new Pattern(pattern1);
        pattern2.addEdge(v2, v0, creates);
        pattern2.reordering();

        Set<GlogueEdge> outEdges2 = gl.getOutEdges(pattern2);
        Set<GlogueEdge> inEdges2 = gl.getInEdges(pattern2);
        Assert.assertEquals(0, outEdges2.size());
        Assert.assertEquals(2, inEdges2.size());

        for (GlogueEdge edge : inEdges2) {
            GlogueExtendIntersectEdge e = (GlogueExtendIntersectEdge) edge;
            Assert.assertEquals(2, e.getSrcPattern().getVertexSet().size());
            ExtendStep step = e.getExtendStep();
            // srcPattern: person1<-person2, extend: person1->software, person2->software
            if (step.getTargetVertexType() == software) {

                Assert.assertEquals(2, step.getExtendEdges().size());
                // edge weight: |person->software| / |person| = 4 / 4 = 1.0
                Assert.assertEquals(
                        1.0, step.getExtendEdges().get(0).getWeight().doubleValue(), delta);
                // step weight: sum(|intermediate results|)/|srcPattern|,
                // i.e., |person1<-person2->software| + |triangle| / |person1->person2| =  (2+1)/2 =
                // 1.5
                Assert.assertEquals(1.5, step.getWeight().doubleValue(), delta);
            }
            // srcPattern: person1->software, extend: person1->person2, software<-person2
            else if (step.getTargetVertexType() == person) {
                Assert.assertEquals(2, step.getExtendEdges().size());
                for (ExtendEdge e1 : step.getExtendEdges()) {
                    if (e1.getEdgeTypeId() == knows) {
                        // edge weight: |person->person| / |person| = 2 / 4 = 0.5
                        Assert.assertEquals(0.5, e1.getWeight().doubleValue(), delta);
                    } else if (e1.getEdgeTypeId() == creates) {
                        // edge weight: |person->software| / |software| = 4 / 2 = 2.0
                        Assert.assertEquals(2.0, e1.getWeight().doubleValue(), delta);
                    }
                }
                // step weight: sum(|intermediate results|)/|srcPattern|,
                // i.e., |person1<-person2->software| + |triangle| / |person1->software| =  (2+1)/4
                // = 0.75
                Assert.assertEquals(0.75, step.getWeight().doubleValue(), delta);
            }
        }
    }
}

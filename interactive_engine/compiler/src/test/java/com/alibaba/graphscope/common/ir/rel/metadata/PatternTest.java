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
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PatternTest {
    static GlogueSchema g = GLogueSchemaTest.mockGlogueSchema();
    static Integer person = 0;
    static Integer software = 1;
    // person -> software
    static EdgeTypeId creates = new EdgeTypeId(0, 1, 1);
    // person -> person
    static EdgeTypeId knows = new EdgeTypeId(0, 0, 0);

    @Test
    public void basic_pattern_order_test() {
        PatternVertex v0 = new SinglePatternVertex(person, 0);
        PatternVertex v1 = new SinglePatternVertex(software, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // test group:

        // p1: person -> person, the two persons are not in the same group
        Pattern p1 = new Pattern();
        p1.addVertex(v0);
        p1.addVertex(v2);
        p1.addEdge(v0, v2, knows);
        p1.reordering();
        Assert.assertNotEquals(p1.getVertexGroup(v0), p1.getVertexGroup(v2));

        // p2: person <-> person, the two persons are in the same group
        Pattern p2 = new Pattern(p1);
        p2.addEdge(v2, v0, knows);
        p2.reordering();
        Assert.assertEquals(p2.getVertexGroup(v0), p2.getVertexGroup(v2));

        // p3: person -> software <- person, the two persons are in the same group
        Pattern p3 = new Pattern();
        p3.addVertex(v0);
        p3.addVertex(v1);
        p3.addVertex(v2);
        p3.addEdge(v0, v1, creates);
        p3.addEdge(v2, v1, creates);
        p3.reordering();
        Assert.assertEquals(p3.getVertexGroup(v0), p3.getVertexGroup(v2));
        Assert.assertNotEquals(p3.getVertexGroup(v0), p3.getVertexGroup(v1));

        // test order:

        // p4: person0->person1->person2, p5: person0->person2->person1, p6:
        // person1->person0->person2
        // the three patterns are isomorphic, and the order of the corresponding
        // vertices must be the same
        PatternVertex v00 = new SinglePatternVertex(person, 0);
        PatternVertex v11 = new SinglePatternVertex(person, 1);
        PatternVertex v22 = new SinglePatternVertex(person, 2);
        Pattern p4 = new Pattern();
        p4.addVertex(v00);
        p4.addVertex(v11);
        p4.addVertex(v22);
        p4.addEdge(v00, v11, knows);
        p4.addEdge(v11, v22, knows);
        p4.reordering();
        Pattern p5 = new Pattern();
        p5.addVertex(v00);
        p5.addVertex(v11);
        p5.addVertex(v22);
        p5.addEdge(v00, v22, knows);
        p5.addEdge(v22, v11, knows);
        p5.reordering();
        Pattern p6 = new Pattern();
        p6.addVertex(v00);
        p6.addVertex(v11);
        p6.addVertex(v22);
        p6.addEdge(v11, v00, knows);
        p6.addEdge(v00, v22, knows);
        p6.reordering();

        Assert.assertEquals(p4.getVertexOrder(v00), p5.getVertexOrder(v00));
        Assert.assertEquals(p4.getVertexOrder(v11), p5.getVertexOrder(v22));
        Assert.assertEquals(p4.getVertexOrder(v22), p5.getVertexOrder(v11));
        Assert.assertEquals(p4.getVertexOrder(v00), p6.getVertexOrder(v11));
        Assert.assertEquals(p4.getVertexOrder(v11), p6.getVertexOrder(v00));
        Assert.assertEquals(p4.getVertexOrder(v22), p6.getVertexOrder(v22));

        Assert.assertEquals(p4.hashCode(), p5.hashCode());
        Assert.assertEquals(p4.hashCode(), p6.hashCode());
    }

    @Test
    public void basic_pattern_extend_test() {
        // person
        PatternVertex v0 = new SinglePatternVertex(person, 0);
        Pattern p0 = new Pattern(v0);
        p0.reordering();
        Assert.assertEquals(1, p0.getVertexNumber().intValue());
        Assert.assertEquals(0, p0.getEdgeNumber().intValue());

        // test getExtendSteps:
        // expected: person -> software, person -> person, person <- person, person <-> person
        List<ExtendStep> steps0 = p0.getExtendSteps(g);
        List<ExtendEdge> edge1 = List.of(new ExtendEdge(0, creates, PatternDirection.OUT));
        List<ExtendEdge> edge2 = List.of(new ExtendEdge(0, knows, PatternDirection.OUT));
        List<ExtendEdge> edge3 = List.of(new ExtendEdge(0, knows, PatternDirection.IN));
        List<ExtendEdge> edge4 =
                List.of(
                        new ExtendEdge(0, knows, PatternDirection.OUT),
                        new ExtendEdge(0, knows, PatternDirection.IN));

        // person -> software
        ExtendStep step1 = new ExtendStep(software, edge1);
        // person -> person(new)
        ExtendStep step2 = new ExtendStep(person, edge2);
        // person <- person(new)
        ExtendStep step3 = new ExtendStep(person, edge3);
        // person <-> person(new)
        ExtendStep step4 = new ExtendStep(person, edge4);
        List<ExtendStep> expectedExtendSteps =
                new ArrayList<>(Arrays.asList(step1, step2, step3, step4));
        expectedExtendSteps.sort((o1, o2) -> o1.toString().compareTo(o2.toString()));
        steps0.sort((o1, o2) -> o1.toString().compareTo(o2.toString()));
        Assert.assertEquals(expectedExtendSteps.toString(), steps0.toString());

        // test extend:
        Pattern p1 = p0.extend(step1);
        Assert.assertEquals(2, p1.getVertexNumber().intValue());
        Assert.assertEquals(1, p1.getEdgeNumber().intValue());
        Assert.assertEquals(software, p1.getVertexById(1).getVertexTypeIds().get(0));
        Pattern p2 = p0.extend(step2);
        Assert.assertEquals(2, p2.getVertexNumber().intValue());
        Assert.assertEquals(1, p2.getEdgeNumber().intValue());
        Assert.assertEquals(person, p2.getVertexById(1).getVertexTypeIds().get(0));
        Pattern p3 = p0.extend(step3);
        Assert.assertEquals(2, p3.getVertexNumber().intValue());
        Assert.assertEquals(1, p3.getEdgeNumber().intValue());
        Assert.assertEquals(person, p3.getVertexById(1).getVertexTypeIds().get(0));
        Assert.assertTrue(p2.isIsomorphicTo(p3));
        Pattern p4 = p0.extend(step4);
        Assert.assertEquals(2, p4.getVertexNumber().intValue());
        Assert.assertEquals(2, p4.getEdgeNumber().intValue());
        Assert.assertEquals(person, p4.getVertexById(1).getVertexTypeIds().get(0));
        Assert.assertFalse(p2.isIsomorphicTo(p4));
    }

    @Test
    public void basic_pattern_isomorphism_test() {
        // v0: software
        // v1: person
        // v2: person
        PatternVertex v0 = new SinglePatternVertex(software, 0);
        PatternVertex v1 = new SinglePatternVertex(person, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // p1 -> s0 <- p2
        Pattern p0 = new Pattern();
        p0.addVertex(v0);
        p0.addVertex(v1);
        p0.addVertex(v2);
        p0.addEdge(v1, v0, creates);
        p0.addEdge(v2, v0, creates);

        // p0 -> s1 <- p2
        Pattern p1 = new Pattern();
        PatternVertex v00 = new SinglePatternVertex(person, 0);
        PatternVertex v11 = new SinglePatternVertex(software, 1);
        p1.addVertex(v00);
        p1.addVertex(v11);
        p1.addVertex(v2);
        p1.addEdge(v00, v11, creates);
        p1.addEdge(v2, v11, creates);
        // p1 -> s0 <- p2 v.s. p0 -> s1 <- p2
        Assert.assertTrue(p0.equals(p1));

        // p1 -> s0 -> p2
        Pattern p2 = new Pattern();
        p2.addVertex(v0);
        p2.addVertex(v1);
        p2.addVertex(v2);
        p2.addEdge(v1, v0, creates);
        p2.addEdge(v0, v2, creates);
        // p1 -> s0 <- p2 v.s. p1 -> s0 -> p2
        Assert.assertFalse(p0.equals(p2));

        // p1 -> s0 <- p2 + p1 -> p2
        Pattern p3 = new Pattern(p0);
        p3.addEdge(v1, v2, knows);
        // p1 -> s0 <- p2 + p1 <- p2
        Pattern p4 = new Pattern(p0);
        p4.addEdge(v2, v1, knows);
        Assert.assertTrue(p3.equals(p4));

        // p2 <- p1 -> s0
        Pattern p5 = new Pattern();
        p5.addVertex(v0);
        p5.addVertex(v1);
        PatternVertex v222 = new SinglePatternVertex(person, 2);
        p5.addVertex(v222);
        p5.addEdge(v1, v0, creates);
        p5.addEdge(v1, v222, knows);
        // p1 -> s0 <-p2
        Pattern p6 = new Pattern();
        p6.addVertex(v0);
        p6.addVertex(v1);
        PatternVertex v2222 = new SinglePatternVertex(software, 2);
        p6.addVertex(v2222);
        p6.addEdge(v1, v0, creates);
        p6.addEdge(v1, v2222, creates);
        Assert.assertFalse(p5.equals(p6));
    }
}

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

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueBasicCardinalityEstimationImpl;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.junit.Assert;
import org.junit.Test;

public class GLogueCardinalityTest {
    static GlogueSchema g = GLogueSchemaTest.mockGlogueSchema();
    static Glogue gl = new Glogue(g, 3);
    static Integer person = 0;
    static Integer software = 1;
    // person -> software
    static EdgeTypeId creates = new EdgeTypeId(0, 1, 1);
    // person -> person
    static EdgeTypeId knows = new EdgeTypeId(0, 0, 0);
    static Double delta = 0.00001;

    @Test
    public void basic_pattern_cardinality_estimation_test() {
        GlogueBasicCardinalityEstimationImpl basicCardinalityEstimation =
                new GlogueBasicCardinalityEstimationImpl(gl, g);
        PatternVertex v0 = new SinglePatternVertex(person, 0);
        PatternVertex v1 = new SinglePatternVertex(software, 1);
        PatternVertex v2 = new SinglePatternVertex(person, 2);

        // low order statistics
        Pattern p0 = new Pattern(v0);
        Pattern p1 = new Pattern(v1);
        Pattern p2 = new Pattern();
        p2.addVertex(v0);
        p2.addVertex(v1);
        p2.addEdge(v0, v1, creates);
        Pattern p3 = new Pattern();
        p3.addVertex(v0);
        p3.addVertex(v2);
        p3.addEdge(v0, v2, knows);
        Assert.assertEquals(
                4.0, basicCardinalityEstimation.getCardinality(p0).doubleValue(), delta);
        Assert.assertEquals(
                2.0, basicCardinalityEstimation.getCardinality(p1).doubleValue(), delta);
        Assert.assertEquals(
                4.0, basicCardinalityEstimation.getCardinality(p2).doubleValue(), delta);
        Assert.assertEquals(
                2.0, basicCardinalityEstimation.getCardinality(p3).doubleValue(), delta);

        // high order statistics
        // sofware<-person0->person2
        Pattern p4 = new Pattern(p2);
        p4.addVertex(v2);
        p4.addEdge(v0, v2, knows);
        p4.reordering();
        Assert.assertEquals(
                2.0, basicCardinalityEstimation.getCardinality(p4).doubleValue(), delta);

        // sofware<-person0->person2 + person2->software
        Pattern p5 = new Pattern(p4);
        p5.addEdge(v2, v1, creates);
        p5.reordering();
        Assert.assertEquals(
                1.0, basicCardinalityEstimation.getCardinality(p5).doubleValue(), delta);

        // person1<-person0->person2
        Pattern p6 = new Pattern(p3);
        PatternVertex v3 = new SinglePatternVertex(person, 3);
        p6.addVertex(v3);
        p6.addEdge(v0, v3, knows);
        p6.reordering();
        Assert.assertEquals(
                1.0, basicCardinalityEstimation.getCardinality(p6).doubleValue(), delta);

        //  person1<-person0->person2 + person1->person2
        Pattern p7 = new Pattern(p6);
        p7.addEdge(v2, v3, knows);
        p7.reordering();
        Assert.assertEquals(
                0.125, basicCardinalityEstimation.getCardinality(p7).doubleValue(), delta);
    }
}

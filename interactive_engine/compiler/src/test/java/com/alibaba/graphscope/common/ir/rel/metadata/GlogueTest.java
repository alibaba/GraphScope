package com.alibaba.graphscope.common.ir.rel.metadata;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueEdge;
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
    static Glogue gl = new Glogue().create(g, 3);

    @Test
    public void glogue_get_row_count_test() {

        // p1 -> s0 <- p2 + p1 -> p2
        // v0: software, v1: person1, v2: person2
        PatternVertex software = new SinglePatternVertex(1, 0);
        PatternVertex person1 = new SinglePatternVertex(0, 1);
        PatternVertex person2 = new SinglePatternVertex(0, 2);
        // etype: person -creates-> software, person-knows->person
        EdgeTypeId creates = new EdgeTypeId(0, 1, 1);
        EdgeTypeId knows = new EdgeTypeId(0, 0, 0);

        // pattern1: person2 <- person1 -> software
        Pattern pattern1 = new Pattern();
        pattern1.addVertex(software);
        pattern1.addVertex(person1);
        pattern1.addVertex(person2);
        pattern1.addEdge(person1, software, creates);
        pattern1.addEdge(person1, person2, knows);
        pattern1.reordering();

        Double delta = 0.00001;

        Double count1 = gl.getRowCount(pattern1);
        Assert.assertEquals(2.0d, count1, delta);

        // pattern2: person2 <- person1 -> software + person2 -> software
        Pattern pattern2 = new Pattern();
        pattern2.addVertex(software);
        pattern2.addVertex(person1);
        pattern2.addVertex(person2);
        pattern2.addEdge(person1, software, creates);
        pattern2.addEdge(person1, person2, knows);
        pattern2.addEdge(person2, software, creates);
        pattern2.reordering();

        Double count2 = gl.getRowCount(pattern2);
        Assert.assertEquals(1.0d, count2, delta);

        // pattern3: person2 <- person1 <- software (not exist)
        Pattern pattern3 = new Pattern();
        pattern3.addVertex(software);
        pattern3.addVertex(person1);
        pattern3.addVertex(person2);
        pattern3.addEdge(software, person1, creates);
        pattern3.addEdge(person1, person2, knows);
        pattern3.reordering();

        Double count3 = gl.getRowCount(pattern3);
        Assert.assertEquals(0.0d, count3, delta);
    }

    @Test
    public void glogue_get_neighbors_test() {

        // p1 -> s0 <- p2 + p1 -> p2
        // v0: software, v1: person1, v2: person2
        PatternVertex software = new SinglePatternVertex(1, 0);
        PatternVertex person1 = new SinglePatternVertex(0, 1);
        PatternVertex person2 = new SinglePatternVertex(0, 2);
        // etype: person -creates-> software, person-knows->person
        EdgeTypeId creates = new EdgeTypeId(0, 1, 1);
        EdgeTypeId knows = new EdgeTypeId(0, 0, 0);

        // pattern1: person2 <- person1 -> software
        Pattern pattern1 = new Pattern();
        pattern1.addVertex(software);
        pattern1.addVertex(person1);
        pattern1.addVertex(person2);
        pattern1.addEdge(person1, software, creates);
        pattern1.addEdge(person1, person2, knows);
        pattern1.reordering();

        Set<GlogueEdge> outEdges = gl.getOutEdges(pattern1);
        Set<GlogueEdge> inEdges = gl.getInEdges(pattern1);
        Assert.assertEquals(0, outEdges.size());
        Assert.assertEquals(2, inEdges.size());

        // pattern2: person2 <- person1 -> software + person2 -> software
        Pattern pattern2 = new Pattern();
        pattern2.addVertex(software);
        pattern2.addVertex(person1);
        pattern2.addVertex(person2);
        pattern2.addEdge(person1, software, creates);
        pattern2.addEdge(person1, person2, knows);
        pattern2.addEdge(person2, software, creates);
        pattern2.reordering();

        outEdges = gl.getOutEdges(pattern2);
        inEdges = gl.getInEdges(pattern2);
        Assert.assertEquals(0, outEdges.size());
        Assert.assertEquals(2, inEdges.size());

        // pattern3: person1 -> person2
        Pattern pattern3 = new Pattern();
        pattern3.addVertex(person1);
        pattern3.addVertex(person2);
        pattern3.addEdge(person1, person2, knows);
        pattern3.reordering();

        outEdges = gl.getOutEdges(pattern3);
        inEdges = gl.getInEdges(pattern3);
        Assert.assertEquals(14, outEdges.size());
        Assert.assertEquals(1, inEdges.size());

        // pattern4: person1 -> software
        Pattern pattern4 = new Pattern();
        pattern4.addVertex(software);
        pattern4.addVertex(person1);
        pattern4.addEdge(person1, software, creates);
        pattern4.reordering();

        outEdges = gl.getOutEdges(pattern4);
        inEdges = gl.getInEdges(pattern4);
        Assert.assertEquals(7, outEdges.size());
        Assert.assertEquals(2, inEdges.size());
    }
}

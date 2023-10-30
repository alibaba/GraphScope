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

package com.alibaba.graphscope.common.ir.glogue;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler.GraphMetadataHandlerProvider;
import com.alibaba.graphscope.common.ir.planner.rules.generative.ExtendIntersectRule;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.ExtVolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class RelMetadataQueryTest {
    @Test
    public void test() throws Exception {
        VolcanoPlanner planner = new ExtVolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster optCluster = GraphOptCluster.create(planner, Utils.rexBuilder);

        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
        GlogueQuery gq = new GlogueQuery(gl, g);

        GraphRelMetadataQuery mq =
                new GraphRelMetadataQuery(
                        new GraphMetadataHandlerProvider(planner, new RelMdRowCount(), gq));

        optCluster.setMetadataQuerySupplier(() -> mq);

        Pattern p = new Pattern();
        // p1 -> s0 <- p2 + p1 -> p2
        PatternVertex v0 = new SinglePatternVertex(1, 0);
        PatternVertex v1 = new SinglePatternVertex(0, 1);
        PatternVertex v2 = new SinglePatternVertex(0, 2);
        // p -> s
        EdgeTypeId e = new EdgeTypeId(0, 1, 1);
        // p -> p
        EdgeTypeId e1 = new EdgeTypeId(0, 0, 0);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v1, v0, e);
        p.addEdge(v2, v0, e);
        p.addEdge(v1, v2, e1);
        p.reordering();
        // System.out.println(gq.getInEdges(p));

        GraphPattern graphPattern = new GraphPattern(optCluster, planner.emptyTraitSet(), p);
        // RelNode topNode = planner.changeTraits(graphPattern,
        // optCluster.traitSet().replace(EnumerableConvention.INSTANCE));
        // System.out.println(topNode.explain());
        planner.setTopDownOpt(true);
        planner.setNoneConventionHasInfiniteCost(false);
        planner.addRule(
                ExtendIntersectRule.Config.DEFAULT
                        .withRelBuilderFactory(GraphPlanner.relBuilderFactory)
                        .withMaxPatternSizeInGlogue(gq.getMaxPatternSize())
                        .toRule());
        planner.setRoot(graphPattern);

        RelNode after = planner.findBestExp();
        planner.dump(new PrintWriter(new FileOutputStream("set1.out"), true));
        System.out.println(after.explain());
        //        System.out.println(mq.getRowCount(graphPattern));
        //        Set<GlogueEdge> glogueEdges = mq.getGlogueEdges(graphPattern);
        //
        //        GlogueEdge first = glogueEdges.iterator().next();
        //        RelNode extendIntersect =
        //                new GraphExtendIntersect(
        //                        optCluster,
        //                        RelTraitSet.createEmpty(),
        //                        new GraphPattern(
        //                                optCluster, RelTraitSet.createEmpty(),
        // first.getSrcPattern()),
        //                        (GlogueExtendIntersectEdge) first);
        //        System.out.println(mq.getNonCumulativeCost(extendIntersect));
    }

    @Test
    public void test_2() {
        Pattern p = new Pattern();
        // p1 -> s0 <- p2 + p1 -> p2
        PatternVertex v0 = new SinglePatternVertex(1, 0);
        PatternVertex v1 = new SinglePatternVertex(0, 1);
        PatternVertex v2 = new SinglePatternVertex(0, 2);
        // p -> s
        EdgeTypeId e = new EdgeTypeId(0, 1, 1);
        // p -> p
        EdgeTypeId e1 = new EdgeTypeId(0, 0, 0);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v1, v0, e);
        p.addEdge(v2, v0, e);
        p.addEdge(v1, v2, e1);
        System.out.println(
                System.identityHashCode(v0)
                        + " "
                        + System.identityHashCode(v1)
                        + " "
                        + System.identityHashCode(v2));

        Pattern p1 = new Pattern(p);
        for (PatternVertex v : p1.getVertexSet()) {
            System.out.println(System.identityHashCode(v));
        }
        // System.out.println(p.removeVertex(v2));
    }
}

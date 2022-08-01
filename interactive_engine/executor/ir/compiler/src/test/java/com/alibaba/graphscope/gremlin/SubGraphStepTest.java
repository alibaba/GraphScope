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

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.AsNoneOp;
import com.alibaba.graphscope.common.intermediate.operator.DedupOp;
import com.alibaba.graphscope.common.intermediate.operator.GetVOp;
import com.alibaba.graphscope.common.intermediate.operator.SubGraphAsUnionOp;
import com.alibaba.graphscope.common.intermediate.process.SinkGraph;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class SubGraphStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_E_subgraph() {
        Traversal traversal = g.E().subgraph("graph_1");
        SubGraphAsUnionOp op =
                (SubGraphAsUnionOp)
                        StepTransformFactory.SUBGRAPH_STEP.apply(traversal.asAdmin().getEndStep());

        Assert.assertEquals("graph_1", op.getGraphConfigs().get(SinkGraph.GRAPH_NAME));
        List<InterOpCollection> actualOps = (List) op.getSubOpCollectionList().get().applyArg();

        // AsNoneOp means identity()
        Class<?> asNoneOp = actualOps.get(0).unmodifiableCollection().get(0).getClass();
        Assert.assertEquals(AsNoneOp.class, asNoneOp);

        // bothV().dedup()
        GetVOp op1 = (GetVOp) actualOps.get(1).unmodifiableCollection().get(0);
        Assert.assertEquals(FfiVOpt.Both, op1.getGetVOpt().get().applyArg());

        DedupOp op2 = (DedupOp) actualOps.get(1).unmodifiableCollection().get(1);
        Assert.assertEquals(
                Collections.singletonList(ArgUtils.asFfiNoneVar()),
                op2.getDedupKeys().get().applyArg());
    }

    @Test
    public void g_V_outE_subgraph() {
        Traversal traversal = g.V().outE().subgraph("graph_1");
        SubGraphAsUnionOp op =
                (SubGraphAsUnionOp)
                        StepTransformFactory.SUBGRAPH_STEP.apply(traversal.asAdmin().getEndStep());

        Assert.assertEquals("graph_1", op.getGraphConfigs().get(SinkGraph.GRAPH_NAME));
        List<InterOpCollection> actualOps = (List) op.getSubOpCollectionList().get().applyArg();

        // AsNoneOp means identity()
        Class<?> asNoneOp = actualOps.get(0).unmodifiableCollection().get(0).getClass();
        Assert.assertEquals(AsNoneOp.class, asNoneOp);

        // bothV().dedup()
        GetVOp op1 = (GetVOp) actualOps.get(1).unmodifiableCollection().get(0);
        Assert.assertEquals(FfiVOpt.Both, op1.getGetVOpt().get().applyArg());

        DedupOp op2 = (DedupOp) actualOps.get(1).unmodifiableCollection().get(1);
        Assert.assertEquals(
                Collections.singletonList(ArgUtils.asFfiNoneVar()),
                op2.getDedupKeys().get().applyArg());
    }
}

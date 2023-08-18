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

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.AsNoneOp;
import com.alibaba.graphscope.common.intermediate.operator.UnionOp;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.gremlin.integration.suite.utils.__;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IdentityStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V() {
        Traversal traversal = g.V().identity();
        Step identityStep = traversal.asAdmin().getEndStep();
        AsNoneOp op = (AsNoneOp) StepTransformFactory.IDENTITY_STEP.apply(identityStep);
        Assert.assertEquals(false, op.getAlias().isPresent());
    }

    @Test
    public void g_E() {
        Traversal traversal = g.E().identity();
        Step identityStep = traversal.asAdmin().getEndStep();
        AsNoneOp op = (AsNoneOp) StepTransformFactory.IDENTITY_STEP.apply(identityStep);
        Assert.assertEquals(false, op.getAlias().isPresent());
    }

    @Test
    public void g_V_union_identity_outE_test() {
        // g.E().union(identity(), bothV())
        Traversal getEdge = g.E();
        Traversal identityTraversal = __.identity();
        Traversal bothVTraversal = __.bothV();
        // Traversal test = g.V().union(identity(), out());

        UnionStep unionStep =
                new UnionStep(
                        getEdge.asAdmin(), identityTraversal.asAdmin(), bothVTraversal.asAdmin());

        Traversal.Admin identityAdmin = (Traversal.Admin) unionStep.getGlobalChildren().get(0);
        Traversal.Admin bothVAdmin = (Traversal.Admin) unionStep.getGlobalChildren().get(1);
        identityAdmin.removeStep(1);
        bothVAdmin.removeStep(1);

        UnionOp op = (UnionOp) StepTransformFactory.UNION_STEP.apply(unionStep);
        List<InterOpCollection> collection =
                (List<InterOpCollection>) op.getSubOpCollectionList().get().applyArg();
        InterOpCollection identityCollection =
                (new InterOpCollectionBuilder(identityTraversal)).build();
        InterOpCollection bothVCollection = (new InterOpCollectionBuilder(bothVTraversal)).build();

        IrPlan identityUnionPlan = new IrPlan(Utils.schemaMeta, collection.get(0));
        String identityUnionJson = identityUnionPlan.getPlanAsJson();

        IrPlan identityPlan = new IrPlan(Utils.schemaMeta, identityCollection);
        String identityJson = identityPlan.getPlanAsJson();

        IrPlan bothVUnionPlan = new IrPlan(Utils.schemaMeta, collection.get(1));
        String bothVUnionJson = bothVUnionPlan.getPlanAsJson();

        IrPlan bothVPlan = new IrPlan(Utils.schemaMeta, bothVCollection);
        String bothVJson = bothVPlan.getPlanAsJson();

        Assert.assertEquals(identityUnionJson, identityJson);
        Assert.assertEquals(bothVUnionJson, bothVJson);
    }

    @Test
    public void g_V_as_identity_test() {
        Traversal traversal = g.V().as("a").identity();
        Step identityStep = traversal.asAdmin().getEndStep();
        AsNoneOp op = (AsNoneOp) StepTransformFactory.IDENTITY_STEP.apply(identityStep);
        Assert.assertEquals(false, op.getAlias().isPresent());
    }
}

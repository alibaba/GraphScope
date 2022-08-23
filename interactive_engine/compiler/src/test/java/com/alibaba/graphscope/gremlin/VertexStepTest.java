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
import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.GetVOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiExpandOpt;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VertexStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    @Test
    public void g_V_out() {
        Traversal traversal = g.V().out();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(FfiExpandOpt.Vertex, op.getExpandOpt().get().applyArg());
    }

    @Test
    public void g_V_outE() {
        Traversal traversal = g.V().outE();
        Step vertexStep = traversal.asAdmin().getEndStep();
        ExpandOp op = (ExpandOp) StepTransformFactory.VERTEX_STEP.apply(vertexStep);
        Assert.assertEquals(FfiDirection.Out, op.getDirection().get().applyArg());
        Assert.assertEquals(FfiExpandOpt.Edge, op.getExpandOpt().get().applyArg());
    }

    // fuse outE + hasLabel
    @Test
    public void g_V_outE_hasLabel() {
        List<InterOpBase> ops = getOps(g.V().outE().hasLabel("knows"));
        // source + expand + sink
        Assert.assertEquals(2, ops.size() - 1);
        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Edge, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals(
                Arrays.asList(ArgUtils.asNameOrId("knows")),
                expandOp.getParams().get().getTables());
    }

    // fuse outE + has("name", ...)
    @Test
    public void g_V_outE_hasProp() {
        List<InterOpBase> ops = getOps(g.V().outE().has("weight", 1.0));
        // source + expand + sink
        Assert.assertEquals(2, ops.size() - 1);
        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Edge, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals("@.weight == 1.0", expandOp.getParams().get().getPredicate().get());
    }

    // fuse outE + has("name", ...) + inV
    @Test
    public void g_V_outE_hasProp_inV() {
        List<InterOpBase> ops = getOps(g.V().outE().has("weight", 1.0).inV());
        // source + expand + sink
        Assert.assertEquals(2, ops.size() - 1);
        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Vertex, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals("@.weight == 1.0", expandOp.getParams().get().getPredicate().get());
    }

    // fuse outE + has("name", ...) + inV
    @Test
    public void g_V_outE_hasProp_inV_hasProp() {
        List<InterOpBase> ops = getOps(g.V().outE().has("weight", 1.0).inV().has("name", "marko"));
        // source + expand + filter + sink
        Assert.assertEquals(3, ops.size() - 1);

        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Vertex, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals("@.weight == 1.0", expandOp.getParams().get().getPredicate().get());

        SelectOp selectOp = (SelectOp) ops.get(2);
        Assert.assertEquals("@.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    // fuse outE + has("name", ...)
    // inV represent as getV
    @Test
    public void g_V_outE_hasProp_as_inV() {
        List<InterOpBase> ops = getOps(g.V().outE().has("weight", 1.0).as("a").inV());
        // source + expand + getV + sink
        Assert.assertEquals(3, ops.size() - 1);

        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Edge, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals("@.weight == 1.0", expandOp.getParams().get().getPredicate().get());
        Assert.assertEquals(ArgUtils.asAlias("a", true), expandOp.getAlias().get().applyArg());

        GetVOp getVOp = (GetVOp) ops.get(2);
        Assert.assertEquals(FfiVOpt.End, getVOp.getGetVOpt().get().applyArg());
    }

    // fuse outE + has("name", ...)
    // inV().has(...) -> getV + filter
    @Test
    public void g_V_outE_hasProp_as_inV_hasProp() {
        List<InterOpBase> ops =
                getOps(g.V().outE().has("weight", 1.0).as("a").inV().has("name", "marko"));
        // source + expand + getV + filter + sink
        Assert.assertEquals(4, ops.size() - 1);

        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Edge, expandOp.getExpandOpt().get().applyArg());
        Assert.assertEquals("@.weight == 1.0", expandOp.getParams().get().getPredicate().get());
        Assert.assertEquals(ArgUtils.asAlias("a", true), expandOp.getAlias().get().applyArg());

        GetVOp op = (GetVOp) ops.get(2);
        Assert.assertEquals(FfiVOpt.End, op.getGetVOpt().get().applyArg());

        SelectOp selectOp = (SelectOp) ops.get(3);
        Assert.assertEquals("@.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    // out + hasLabel -> expand + filter
    @Test
    public void g_V_out_hasLabel() {
        List<InterOpBase> ops = getOps(g.V().out().hasLabel("person"));
        // source + expand + filter + sink
        Assert.assertEquals(3, ops.size() - 1);

        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Vertex, expandOp.getExpandOpt().get().applyArg());

        SelectOp selectOp = (SelectOp) ops.get(2);
        Assert.assertEquals("@.~label == \"person\"", selectOp.getPredicate().get().applyArg());
    }

    // out + has("name", ...) -> expand + filter
    @Test
    public void g_V_out_hasProp() {
        List<InterOpBase> ops = getOps(g.V().out().has("name", "marko"));
        // source + expand + filter + sink
        Assert.assertEquals(3, ops.size() - 1);

        ExpandOp expandOp = (ExpandOp) ops.get(1);
        Assert.assertEquals(FfiExpandOpt.Vertex, expandOp.getExpandOpt().get().applyArg());

        SelectOp selectOp = (SelectOp) ops.get(2);
        Assert.assertEquals("@.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    public static List<InterOpBase> getOps(Traversal traversal) {
        IrStandardOpProcessor.applyStrategies(traversal);
        InterOpCollection opCollection = new InterOpCollectionBuilder(traversal).build();
        InterOpCollection.applyStrategies(opCollection);
        InterOpCollection.process(opCollection);
        return opCollection.unmodifiableCollection();
    }
}

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
import com.alibaba.graphscope.common.intermediate.MatchSentence;
import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.GetVOp;
import com.alibaba.graphscope.common.intermediate.operator.MatchOp;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.gremlin.plugin.processor.IrStandardOpProcessor;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MatchStepTest {
    private Graph graph = TinkerFactory.createModern();
    private GraphTraversalSource g = graph.traversal();

    private List<MatchSentence> getSentences(Traversal traversal) {
        MatchStep matchStep = (MatchStep) traversal.asAdmin().getEndStep();
        MatchOp matchOp = (MatchOp) StepTransformFactory.MATCH_STEP.apply(matchStep);
        return (List<MatchSentence>) matchOp.getSentences().get().applyArg();
    }

    private boolean isEqualWith(
            MatchSentence sentence,
            String expectedStart,
            String expectedEnd,
            FfiJoinKind expectedJoin,
            int expectedBinderSize) {
        return sentence.getStartTag().equals(ArgUtils.asFfiAlias(expectedStart, true))
                && sentence.getEndTag().equals(ArgUtils.asFfiAlias(expectedEnd, true))
                && sentence.getJoinKind() == expectedJoin
                && sentence.getBinders().unmodifiableCollection().size() == expectedBinderSize;
    }

    @Test
    public void g_V_match_as_a_out_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").out().as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 1));
    }

    // out + hasLabel("person") -> expand + filter
    @Test
    public void g_V_match_as_a_out_hasLabel_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").out().hasLabel("person").as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 2));

        ExpandOp op = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(
                "@.~label && @.~label == \"person\"", selectOp.getPredicate().get().applyArg());
    }

    // out + has("name", "marko") -> expand + filter
    @Test
    public void g_V_match_as_a_out_hasProp_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").out().has("name", "marko").as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 2));

        ExpandOp op = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(
                "@.name && @.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    // fuse outE + hasLabel("knows")
    @Test
    public void g_V_match_as_a_outE_hasLabel_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").outE().hasLabel("knows").as("b"));
        IrStandardOpProcessor.applyStrategies(traversal);

        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 1));
        ExpandOp op = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals("knows", op.getParams().get().getTables().get(0).name);
        Assert.assertEquals(true, op.getIsEdge().get().applyArg());
    }

    // fuse outE + has("weight", 1.0)
    @Test
    public void g_V_match_as_a_outE_hasProp_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").outE().has("weight", 1.0).as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 1));
        ExpandOp op = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals(
                "@.weight && @.weight == 1.0", op.getParams().get().getPredicate().get());
        Assert.assertEquals(true, op.getIsEdge().get().applyArg());
    }

    // outV + hasLabel("person") -> getV + filter
    @Test
    public void g_V_match_as_a_outE_outV_hasLabel_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").outE().outV().hasLabel("person").as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 3));

        GetVOp op = (GetVOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(FfiVOpt.Start, op.getGetVOpt().get().applyArg());

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(2);
        Assert.assertEquals(
                "@.~label && @.~label == \"person\"", selectOp.getPredicate().get().applyArg());
    }

    // fuse outV + has("name", "marko")
    @Test
    public void g_V_match_as_a_outE_outV_hasProp_as_b_test() {
        Traversal traversal = g.V().match(__.as("a").outE().outV().has("name", "marko").as("b"));
        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 3));

        GetVOp op = (GetVOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(FfiVOpt.Start, op.getGetVOpt().get().applyArg());

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(2);
        Assert.assertEquals(
                "@.name && @.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    // fuse outE + hasLabel("knows") + inV()
    // has("name", "marko") -> filter
    @Test
    public void g_V_match_as_a_outE_hasLabel_inV_hasProp_as_b_test() {
        Traversal traversal =
                g.V().match(__.as("a").outE().hasLabel("knows").inV().has("name", "marko").as("b"));
        IrStandardOpProcessor.applyStrategies(traversal);

        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 2));

        ExpandOp op = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals(false, op.getIsEdge().get().applyArg());
        Assert.assertEquals("knows", op.getParams().get().getTables().get(0).name);

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(
                "@.name && @.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    // fuse outE + has("weight", 1.0) + inV()
    // has("name", "marko") -> filter
    @Test
    public void g_V_match_as_a_outE_hasProp_inV_hasProp_as_b_test() {
        Traversal traversal =
                g.V().match(
                                __.as("a")
                                        .outE()
                                        .has("weight", 1.0)
                                        .inV()
                                        .has("name", "marko")
                                        .as("b"));

        MatchSentence sentence = getSentences(traversal).get(0);
        Assert.assertTrue(isEqualWith(sentence, "a", "b", FfiJoinKind.Inner, 2));

        ExpandOp expandOp = (ExpandOp) sentence.getBinders().unmodifiableCollection().get(0);
        Assert.assertEquals(
                "@.weight && @.weight == 1.0", expandOp.getParams().get().getPredicate().get());
        Assert.assertEquals(false, expandOp.getIsEdge().get().applyArg());

        SelectOp selectOp = (SelectOp) sentence.getBinders().unmodifiableCollection().get(1);
        Assert.assertEquals(
                "@.name && @.name == \"marko\"", selectOp.getPredicate().get().applyArg());
    }

    @Test
    public void g_V_match_as_a_out_as_b_out_as_c_test() {
        Traversal traversal = g.V().match(__.as("a").out().as("b"), __.as("b").out().as("c"));
        MatchSentence sentence1 = getSentences(traversal).get(0);
        MatchSentence sentence2 = getSentences(traversal).get(1);
        Assert.assertTrue(isEqualWith(sentence1, "a", "b", FfiJoinKind.Inner, 1));
        Assert.assertTrue(isEqualWith(sentence2, "b", "c", FfiJoinKind.Inner, 1));
    }

    @Test
    public void g_V_match_as_a_out_as_b_where_test() {
        Traversal traversal =
                g.V().match(__.as("a").out().as("b"), __.where(__.as("a").out().as("c")));
        MatchSentence sentence1 = getSentences(traversal).get(0);
        MatchSentence sentence2 = getSentences(traversal).get(1);
        Assert.assertTrue(isEqualWith(sentence1, "a", "b", FfiJoinKind.Inner, 1));
        Assert.assertTrue(isEqualWith(sentence2, "a", "c", FfiJoinKind.Semi, 1));
    }

    @Test
    public void g_V_match_as_a_out_as_b_not_test() {
        Traversal traversal =
                g.V().match(__.as("a").out().as("b"), __.not(__.as("a").out().as("c")));
        MatchSentence sentence1 = getSentences(traversal).get(0);
        MatchSentence sentence2 = getSentences(traversal).get(1);
        Assert.assertTrue(isEqualWith(sentence1, "a", "b", FfiJoinKind.Inner, 1));
        Assert.assertTrue(isEqualWith(sentence2, "a", "c", FfiJoinKind.Anti, 1));
    }
}

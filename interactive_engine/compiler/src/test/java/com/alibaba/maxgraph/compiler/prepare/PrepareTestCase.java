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
package com.alibaba.maxgraph.compiler.prepare;

import com.alibaba.maxgraph.compiler.CompilerBaseTest;
import org.junit.Test;

public class PrepareTestCase extends CompilerBaseTest {
    private DefaultGraphPrepare graphPrepare = new DefaultGraphPrepare();

    public PrepareTestCase() throws Exception {
    }

    @Test
    public void testPrepareSource() {
//        PreparedTraversal preparedTraversal = graphPrepare.prepare("prepare1", g.V("@1").out());
//        GraphTraversal graphTraversal = preparedTraversal.getTraversal();
//        ExpressionTree expressionTree = ExpressionFactory.newFactory().valueOf(graphTraversal, schema);
//        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
//        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(
//                GremlinQueryPlanBuilder.newPlanBuilder().build(expressionTree, new OptimizeConfig()).getQueryPlan());
//        OperatorListManager operatorListManager = queryFlowManager.getOperatorListManager();
//
//        Assert.assertEquals(1, operatorListManager.getPrepareEntityList().size());
//        PrepareEntity prepareEntity = operatorListManager.getPrepareEntityList().get(0);
//        Assert.assertEquals(1, prepareEntity.getArgumentIndex());
//        Assert.assertEquals(Lists.newArrayList(1), prepareEntity.getParamIndexList());
//
//        PreparedExecuteParam preparedExecuteParam = graphPrepare.execute("prepare1", graphPrepare.param(1L, 2L, 3L));
//        List<List<Object>> expectParamList = Lists.newArrayList();
//        expectParamList.add(Lists.newArrayList(1L, 2L, 3L));
//        Assert.assertEquals(expectParamList, preparedExecuteParam.getParamList());
//        Message.Value paramValue = (Message.Value) prepareEntity.prepareParam(preparedExecuteParam.getParamList(), schema, new CompilerConfig());
//        Assert.assertEquals(Message.Value.newBuilder().setIndex(1).addAllLongValueList(Lists.newArrayList(1L, 2L, 3L)).build(), paramValue);
    }

    @Test
    public void testPrepareCompare1() {
//        PreparedTraversal preparedTraversal = graphPrepare.prepare("prepare1", g.V().has("@2", "@1").out());
//        GraphTraversal graphTraversal = preparedTraversal.getTraversal();
//        ExpressionTree expressionTree = ExpressionFactory.newFactory().valueOf(graphTraversal, schema);
//        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
//        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(
//                GremlinQueryPlanBuilder.newPlanBuilder().build(expressionTree, new OptimizeConfig()).getQueryPlan());
//        OperatorListManager operatorListManager = queryFlowManager.getOperatorListManager();
//        Assert.assertEquals(1, operatorListManager.getPrepareEntityList().size());
//        PrepareEntity prepareEntity = operatorListManager.getPrepareEntityList().get(0);
//        Assert.assertEquals(1, prepareEntity.getArgumentIndex());
//        Assert.assertEquals(Lists.newArrayList(2, 1), prepareEntity.getParamIndexList());
//
//        PreparedExecuteParam preparedExecuteParam = graphPrepare.execute("prepare1",
//                graphPrepare.param("tom"),
//                graphPrepare.param("firstname"));
//        List<List<Object>> expectParamList = Lists.newArrayList();
//        expectParamList.add(Lists.newArrayList("tom"));
//        expectParamList.add(Lists.newArrayList("firstname"));
//        Assert.assertEquals(expectParamList, preparedExecuteParam.getParamList());
//        QueryFlowOuterClass.LogicalCompare logicalCompare = (QueryFlowOuterClass.LogicalCompare) prepareEntity.prepareParam(preparedExecuteParam.getParamList(), schema, new CompilerConfig());
////        Assert.assertEquals(QueryFlowOuterClass.LogicalCompare.newBuilder().setPropId(schema.getPropertyDef("firstname").get().id).setCompare(QueryFlowOuterClass.CompareType.EQ).setValue(
////                Message.Value.newBuilder().setStrValue("tom").build()).setType(Message.VariantType.VT_STRING).setIndex(1).build(),
////                logicalCompare);
    }

    @Test
    public void testPrepareCompare2() {
//        PreparedTraversal preparedTraversal = graphPrepare.prepare("prepare1", g.V().has("firstname", "@1").out());
//        GraphTraversal graphTraversal = preparedTraversal.getTraversal();
//        ExpressionTree expressionTree = ExpressionFactory.newFactory().valueOf(graphTraversal, schema);
//        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
//        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(
//                GremlinQueryPlanBuilder.newPlanBuilder().build(expressionTree, new OptimizeConfig()).getQueryPlan());
//        OperatorListManager operatorListManager = queryFlowManager.getOperatorListManager();
//        Assert.assertEquals(1, operatorListManager.getPrepareEntityList().size());
//        PrepareEntity prepareEntity = operatorListManager.getPrepareEntityList().get(0);
//        Assert.assertEquals(1, prepareEntity.getArgumentIndex());
//        Assert.assertEquals(Lists.newArrayList(1), prepareEntity.getParamIndexList());
//
//        PreparedExecuteParam preparedExecuteParam = graphPrepare.execute("prepare1",
//                graphPrepare.param("tom"));
//        List<List<Object>> expectParamList = Lists.newArrayList();
//        expectParamList.add(Lists.newArrayList("tom"));
//        Assert.assertEquals(expectParamList, preparedExecuteParam.getParamList());
//        QueryFlowOuterClass.LogicalCompare logicalCompare = (QueryFlowOuterClass.LogicalCompare) prepareEntity.prepareParam(preparedExecuteParam.getParamList(), schema, new CompilerConfig());
////        Assert.assertEquals(QueryFlowOuterClass.LogicalCompare.newBuilder().setPropId(schema.getPropertyDef("firstname").get().id).setCompare(QueryFlowOuterClass.CompareType.EQ).setValue(
////                Message.Value.newBuilder().setStrValue("tom").build()).setType(Message.VariantType.VT_STRING).setIndex(1).build(),
////                logicalCompare);
    }

    @Test
    public void testPrepareValueCompare() {
//        PreparedTraversal preparedTraversal = graphPrepare.prepare("prepare1", g.V("@1").has("firstname", "@2").out());
//        GraphTraversal graphTraversal = preparedTraversal.getTraversal();
//        ExpressionTree expressionTree = ExpressionFactory.newFactory().valueOf(graphTraversal, schema);
//        QueryFlowBuilder queryFlowBuilder = new QueryFlowBuilder();
//        QueryFlowManager queryFlowManager = queryFlowBuilder.prepareQueryFlow(
//                GremlinQueryPlanBuilder.newPlanBuilder().build(expressionTree, new OptimizeConfig()).getQueryPlan());
//        OperatorListManager operatorListManager = queryFlowManager.getOperatorListManager();
//        Assert.assertEquals(2, operatorListManager.getPrepareEntityList().size());
//
//        PrepareEntity prepareEntity1 = operatorListManager.getPrepareEntityList().get(0);
//        Assert.assertEquals(1, prepareEntity1.getArgumentIndex());
//        Assert.assertEquals(Lists.newArrayList(1), prepareEntity1.getParamIndexList());
//
//        PrepareEntity prepareEntity2 = operatorListManager.getPrepareEntityList().get(1);
//        Assert.assertEquals(2, prepareEntity2.getArgumentIndex());
//        Assert.assertEquals(Lists.newArrayList(2), prepareEntity2.getParamIndexList());
//
//        PreparedExecuteParam preparedExecuteParam = graphPrepare.execute("prepare1",
//                graphPrepare.param(1L, 2L, 3L),
//                graphPrepare.param("tom"));
//
//        List<List<Object>> expectParamList1 = Lists.newArrayList();
//        expectParamList1.add(Lists.newArrayList(1L, 2L, 3L));
//        expectParamList1.add(Lists.newArrayList("tom"));
//        Assert.assertEquals(expectParamList1, preparedExecuteParam.getParamList());
//
//        Message.Value paramValue = (Message.Value) prepareEntity1.prepareParam(preparedExecuteParam.getParamList(), schema, new CompilerConfig());
//        Assert.assertEquals(Message.Value.newBuilder().setIndex(1).addAllLongValueList(Lists.newArrayList(1L, 2L, 3L)).build(), paramValue);
//
//        QueryFlowOuterClass.LogicalCompare logicalCompare = (QueryFlowOuterClass.LogicalCompare) prepareEntity2.prepareParam(preparedExecuteParam.getParamList(), schema, new CompilerConfig());
////        Assert.assertEquals(QueryFlowOuterClass.LogicalCompare.newBuilder().setPropId(schema.getPropertyDef("firstname").get().id).setCompare(QueryFlowOuterClass.CompareType.EQ).setValue(
////                Message.Value.newBuilder().setStrValue("tom").build()).setType(Message.VariantType.VT_STRING).setIndex(2).build(),
////                logicalCompare);
    }
}

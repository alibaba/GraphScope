///*
// * Copyright 2020 Alibaba Group Holding Limited.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.alibaba.graphscope.common.ir.runtime;
//
//import com.alibaba.graphscope.common.ir.IrUtils;
//import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
//import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
//import com.alibaba.graphscope.common.ir.tools.config.*;
//import com.alibaba.graphscope.gaia.proto.OuterExpression;
//
//import org.apache.calcite.rex.RexNode;
//import org.junit.Test;
//
//public class ConverterTest {
//    @Test
//    public void expression_test() {
//        RexToProtoConverter converter = new RexToProtoConverter(true, true);
//
//        GraphBuilder graphBuilder = IrUtils.mockGraphBuilder();
//        graphBuilder
//                .source(
//                        new SourceConfig(
//                                GraphOpt.Source.VERTEX,
//                                new LabelConfig(false).addLabel("person"),
//                                "x"))
//                .expand(
//                        new ExpandConfig(
//                                GraphOpt.Expand.OUT, new LabelConfig(false).addLabel("knows"), "y"))
//                .getV(
//                        new GetVConfig(
//                                GraphOpt.GetV.END, new LabelConfig(false).addLabel("person"), "z"));
//        //        RexNode rexNode =
//        //                graphBuilder.call(
//        //                        GraphStdOperatorTable.GREATER_THAN,
//        //                        graphBuilder.call(
//        //                                GraphStdOperatorTable.MULTIPLY,
//        //                                graphBuilder.call(
//        //                                        GraphStdOperatorTable.PLUS,
//        //                                        graphBuilder.variable("x", "age"),
//        //                                        graphBuilder.variable("y", "weight")),
//        //                                graphBuilder.variable("z", "age")),
//        //                        graphBuilder.call(
//        //                                GraphStdOperatorTable.PLUS,
//        //                                graphBuilder.variable("z", "age"),
//        //                                graphBuilder.literal(10)));
//
//        //                RexNode var1 = graphBuilder.variable("x", "age");
//        //                RexNode var2 = graphBuilder.variable("y", "weight");
//        //                RexNode var3 = graphBuilder.variable("z", "age");
//        //                RexNode gt_1 = graphBuilder.call(GraphStdOperatorTable.GREATER_THAN, var1,
//        // var2);
//        //                RexNode lt_1 = graphBuilder.call(GraphStdOperatorTable.LESS_THAN, var2,
//        // var3);
//        //                RexNode eq_1 = graphBuilder.call(GraphStdOperatorTable.EQUALS, var1,
//        // var3);
//        //                RexNode rexNode = graphBuilder.call(
//        //                        GraphStdOperatorTable.AND,
//        //                        graphBuilder.call(GraphStdOperatorTable.OR, gt_1, lt_1),
//        //
//        //                        eq_1);
//        //                RexNode rexNode = graphBuilder.literal(1);
//        RexNode rexNode = graphBuilder.variable("x");
//        OuterExpression.Expression expression = rexNode.accept(converter);
//        System.out.println(expression.getOperatorsList());
//
//        //        FfiResult.ByValue res = IrCoreLibrary.INSTANCE.setExpr(new
//        // FfiPbPointer.ByValue(exprBuilder.build().toByteArray()));
//    }
//}

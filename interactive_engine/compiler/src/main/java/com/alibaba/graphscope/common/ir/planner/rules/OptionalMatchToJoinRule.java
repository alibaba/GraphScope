///*
// *
// *  * Copyright 2020 Alibaba Group Holding Limited.
// *  *
// *  * Licensed under the Apache License, Version 2.0 (the "License");
// *  * you may not use this file except in compliance with the License.
// *  * You may obtain a copy of the License at
// *  *
// *  * http://www.apache.org/licenses/LICENSE-2.0
// *  *
// *  * Unless required by applicable law or agreed to in writing, software
// *  * distributed under the License is distributed on an "AS IS" BASIS,
// *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  * See the License for the specific language governing permissions and
// *  * limitations under the License.
// *
// */
//
//package com.alibaba.graphscope.common.ir.planner.rules;
//
//import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
//import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
//import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
//import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
//import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
//
//import org.apache.calcite.plan.RelOptRuleCall;
//import org.apache.calcite.plan.RelRule;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.tools.RelBuilderFactory;
//import org.checkerframework.checker.nullness.qual.Nullable;
//
//public class OptionalMatchToJoinRule<C extends OptionalMatchToJoinRule.Config> extends RelRule {
//    protected OptionalMatchToJoinRule(C config) {
//        super(config);
//    }
//
//    @Override
//    public void onMatch(RelOptRuleCall call) {
//        RelNode left = call.rel(1);
//        AbstractLogicalMatch match = call.rel(0);
//        GraphBuilder graphBuilder = (GraphBuilder) call.builder();
//        RelNode right = match;
//        if (match instanceof GraphLogicalSingleMatch
//                && ((GraphLogicalSingleMatch) match).getMatchOpt() != GraphOpt.Match.INNER) {
//            right =
//                    graphBuilder
//                            .match(
//                                    ((GraphLogicalSingleMatch) match).getSentence(),
//                                    GraphOpt.Match.INNER)
//                            .build();
//        }
//        call.transformTo(
//                graphBuilder
//                        .push(left)
//                        .push(right)
//                        .join(getJoinRelType(match), graphBuilder.getJoinCondition(left, right))
//                        .build());
//    }
//
//    private JoinRelType getJoinRelType(AbstractLogicalMatch match) {
//        if (match instanceof GraphLogicalMultiMatch) {
//            return JoinRelType.INNER;
//        } else {
//            GraphLogicalSingleMatch singleMatch = (GraphLogicalSingleMatch) match;
//            switch (singleMatch.getMatchOpt()) {
//                case ANTI:
//                    return JoinRelType.ANTI;
//                case OPTIONAL:
//                    return JoinRelType.LEFT;
//                case INNER:
//                default:
//                    return JoinRelType.INNER;
//            }
//        }
//    }
//
//    public static class Config implements RelRule.Config {
//        public static OptionalMatchToJoinRule.Config DEFAULT =
//                new OptionalMatchToJoinRule.Config()
//                        .withOperandSupplier(
//                                b0 ->
//                                        b0.operand(AbstractLogicalMatch.class)
//                                                .oneInput(
//                                                        b1 ->
//                                                                b1.operand(RelNode.class)
//                                                                        .anyInputs()))
//                        .withDescription("OptionalMatchToJoinRule");
//
//        private RelRule.OperandTransform operandSupplier;
//        private @Nullable String description;
//        private RelBuilderFactory builderFactory;
//
//        @Override
//        public RelRule toRule() {
//            return new OptionalMatchToJoinRule(this);
//        }
//
//        @Override
//        public OptionalMatchToJoinRule.Config withRelBuilderFactory(
//                RelBuilderFactory relBuilderFactory) {
//            this.builderFactory = relBuilderFactory;
//            return this;
//        }
//
//        @Override
//        public OptionalMatchToJoinRule.Config withDescription(
//                @org.checkerframework.checker.nullness.qual.Nullable String s) {
//            this.description = s;
//            return this;
//        }
//
//        @Override
//        public OptionalMatchToJoinRule.Config withOperandSupplier(
//                RelRule.OperandTransform operandTransform) {
//            this.operandSupplier = operandTransform;
//            return this;
//        }
//
//        @Override
//        public RelRule.OperandTransform operandSupplier() {
//            return this.operandSupplier;
//        }
//
//        @Override
//        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
//            return this.description;
//        }
//
//        @Override
//        public RelBuilderFactory relBuilderFactory() {
//            return this.builderFactory;
//        }
//    }
//}

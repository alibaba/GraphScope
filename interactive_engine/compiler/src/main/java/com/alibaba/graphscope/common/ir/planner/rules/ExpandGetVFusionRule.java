/*
 * Copyright 2023 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.schema.GraphOptTable;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalGetV;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

// This rule try to fuse GraphLogicalExpand and GraphLogicalGetV if GraphLogicalExpand has no alias
// (that it won't be visited individually later):
// 1. if GraphLogicalGetV has no filters, then:
//      GraphLogicalExpand + GraphLogicalGetV -> GraphPhysicalExpand(ExpandV),
// where GraphPhysicalExpand carries the alias of GraphLogicalGetV
// 2. if GraphLogicalGetV has filters, then:
//      GraphLogicalExpand + GraphLogicalGetV -> GraphPhysicalExpand(ExpandV)  +
// GraphPhysicalGetV(VertexFilter),
// where GraphPhysicalExpand carries the Default alias, and GraphPhysicalGetV carries the alias of
// GraphLogicalGetV
public abstract class ExpandGetVFusionRule<C extends RelRule.Config> extends RelRule<C>
        implements TransformationRule {

    protected ExpandGetVFusionRule(C config) {
        super(config);
    }

    protected RelNode transform(GraphLogicalGetV getV, GraphLogicalExpand expand, RelNode input) {
        if (expand.getOpt().equals(GraphOpt.Expand.OUT) && getV.getOpt().equals(GraphOpt.GetV.END)
                || expand.getOpt().equals(GraphOpt.Expand.IN)
                        && getV.getOpt().equals(GraphOpt.GetV.START)
                || expand.getOpt().equals(GraphOpt.Expand.BOTH)
                        && getV.getOpt().equals(GraphOpt.GetV.OTHER)) {
            if (canFuse(getV, expand)) {
                GraphPhysicalExpand physicalExpand =
                        GraphPhysicalExpand.create(
                                expand.getCluster(),
                                expand.getHints(),
                                input,
                                expand,
                                getV,
                                GraphOpt.PhysicalExpandOpt.VERTEX,
                                getV.getAliasName());
                return physicalExpand;
            } else {
                GraphPhysicalExpand physicalExpand =
                        GraphPhysicalExpand.create(
                                expand.getCluster(),
                                expand.getHints(),
                                input,
                                expand,
                                getV,
                                GraphOpt.PhysicalExpandOpt.VERTEX,
                                AliasInference.DEFAULT_NAME);
                // If with filters, then create a GraphPhysicalGetV to do the filtering.
                // We set alias of getV to null to avoid alias conflict (with expand's alias)
                GraphPhysicalGetV physicalGetV =
                        GraphPhysicalGetV.create(
                                getV.getCluster(),
                                getV.getHints(),
                                physicalExpand,
                                getV,
                                getV.getAliasName(),
                                GraphOpt.PhysicalGetVOpt.ITSELF);
                return physicalGetV;
            }
        } else {
            return getV;
        }
    }

    private boolean canFuse(GraphLogicalGetV getV, GraphLogicalExpand expand) {
        // If GraphLogicalGetV has filters (or need to cache properties), then we cannot fuse them
        // directly. Instead, we create a
        // EdgeExpand(V) with an Auxilia for the filters.
        // todo: currently, we have not considered the property cache case yet. We will add this
        // after FieldTrimmer is ready.
        // If GraphLogicalGetV has no filters (or need to cache properties), then:
        // 1. if we want to expand "person-knows->person", and the type in getV is "person",
        // we can fuse them into one EdgeExpand with type "knows" (where "knows" will be given in
        // EdgeExpand's QueryParam in PhysicalPlan)
        // 2. if we want to expand "person-create->post", while in schema, a "create" actually
        // consists of "person-create->post" and "person-create->comment",
        // we do not fuse them directly. Instead, we create a EdgeExpand(V) with type "create" and
        // an Auxilia with type "post" as the filter.
        // 3. if we want to expand "person-islocatedin->city", while in schema, a "islocatedin"
        // actually
        // consists of "person-islocatedin->city" and "post-islocatedin->country".
        // Thought the edge type of "islocatedin" may generate "city" and "country", we can still
        // fuse them into a single EdgeExpand(V) with type "islocatedin" directly if we can confirm
        // that the expand starts from "person".
        // 4. a special case is that, currently, for gremlin query like g.V().out("create"), we have
        // not infer precise types for getV yet (getV may contain all vertex types).
        // In this case, if getV's types contains all the types that expand will generate, we can
        // fuse them.

        Set<Integer> edgeExpandedVLabels = new HashSet<>();
        // the optTables in expand preserves the full schema information for the edges,
        // that is, for edge type "create", it contains both "person-create->post" and
        // "person-create->comment", "user-create->post" etc.
        List<RelOptTable> optTables = expand.getTableConfig().getTables();
        // the edgeParamLabels in expand preserves the inferred schema information for the edges,
        // that is, for edge type "create", it contains only "person-create->post" if user queries
        // like g.V().hasLabel("person").out("create").hasLabel("post")
        GraphLabelType edgeParamType =
                com.alibaba.graphscope.common.ir.tools.Utils.getGraphLabels(expand.getRowType());
        List<GraphLabelType.Entry> edgeParamLabels = edgeParamType.getLabelsEntry();
        GraphOpt.Expand direction = expand.getOpt();
        // First, we get all the source vertex types where the edge will be expanded from.
        // e.g., expand from "person"
        Set<Integer> edgeExpandedSrcVLabels = new HashSet<>();
        for (GraphLabelType.Entry edgeLabel : edgeParamLabels) {
            switch (direction) {
                case OUT:
                    edgeExpandedSrcVLabels.add(edgeLabel.getSrcLabelId());
                    break;
                case IN:
                    edgeExpandedSrcVLabels.add(edgeLabel.getDstLabelId());
                    break;
                case BOTH:
                    edgeExpandedSrcVLabels.add(edgeLabel.getDstLabelId());
                    edgeExpandedSrcVLabels.add(edgeLabel.getSrcLabelId());
                    break;
            }
        }
        // Then, we get all the destination vertex types where the edge will be expanded to.
        // e.g., expand "likes"
        for (RelOptTable optTable : optTables) {
            if (optTable instanceof GraphOptTable) {
                GraphOptTable graphOptTable = (GraphOptTable) optTable;
                List<GraphLabelType.Entry> edgeUserGivenParamLabels =
                        com.alibaba.graphscope.common.ir.tools.Utils.getGraphLabels(
                                        graphOptTable.getRowType())
                                .getLabelsEntry();
                for (GraphLabelType.Entry edgeLabel : edgeUserGivenParamLabels) {
                    switch (direction) {
                        case OUT:
                            if (edgeExpandedSrcVLabels.contains(edgeLabel.getSrcLabelId())) {
                                edgeExpandedVLabels.add(edgeLabel.getDstLabelId());
                            }
                            break;
                        case IN:
                            if (edgeExpandedSrcVLabels.contains(edgeLabel.getDstLabelId())) {
                                edgeExpandedVLabels.add(edgeLabel.getSrcLabelId());
                            }
                            break;
                        case BOTH:
                            if (edgeExpandedSrcVLabels.contains(edgeLabel.getSrcLabelId())) {
                                edgeExpandedVLabels.add(edgeLabel.getDstLabelId());
                            }
                            if (edgeExpandedSrcVLabels.contains(edgeLabel.getDstLabelId())) {
                                edgeExpandedVLabels.add(edgeLabel.getSrcLabelId());
                            }
                            break;
                    }
                }
            }
        }

        // Finally, we check if the vertex types in getV to see if the type filter for the expanded
        // vertex is necessary.
        //  e.g., if getV is "post" and expand type is "likes", then we cannot fuse them directly.
        // Instead, we should create an EdgeExpand(V) with type "likes" and an Auxilia with type
        // "post" as the filter.
        List<GraphLabelType.Entry> vertexParamLabels =
                com.alibaba.graphscope.common.ir.tools.Utils.getGraphLabels(getV.getRowType())
                        .getLabelsEntry();
        Set<Integer> vertexExpandedVLabels = new HashSet<>();
        for (GraphLabelType.Entry vertexLabel : vertexParamLabels) {
            vertexExpandedVLabels.add(vertexLabel.getLabelId());
        }
        return ObjectUtils.isEmpty(getV.getFilters())
                && vertexExpandedVLabels.containsAll(edgeExpandedVLabels);
    }

    // transform expande + getv to GraphPhysicalExpandGetV
    public static class BasicExpandGetVFusionRule
            extends ExpandGetVFusionRule<BasicExpandGetVFusionRule.Config> {
        protected BasicExpandGetVFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalGetV getV = call.rel(0);
            GraphLogicalExpand expand = call.rel(1);
            call.transformTo(transform(getV, expand, expand.getInput(0)));
        }

        public static class Config implements RelRule.Config {
            public static BasicExpandGetVFusionRule.Config DEFAULT =
                    new Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalGetV.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(
                                                                                    GraphLogicalExpand
                                                                                            .class)
                                                                            .predicate(
                                                                                    (GraphLogicalExpand
                                                                                                    expand) -> {
                                                                                        int alias =
                                                                                                expand
                                                                                                        .getAliasId();
                                                                                        return alias
                                                                                                == AliasInference
                                                                                                        .DEFAULT_ID;
                                                                                    })
                                                                            .anyInputs()));

            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public RelRule toRule() {
                return new BasicExpandGetVFusionRule(this);
            }

            @Override
            public Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public Config withOperandSupplier(OperandTransform operandTransform) {
                this.operandSupplier = operandTransform;
                return this;
            }

            @Override
            public OperandTransform operandSupplier() {
                return this.operandSupplier;
            }

            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable String description() {
                return this.description;
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }

    // transform expande + getv to GraphPhysicalExpandGetV in PathExpand
    public static class PathBaseExpandGetVFusionRule
            extends ExpandGetVFusionRule<PathBaseExpandGetVFusionRule.Config> {
        protected PathBaseExpandGetVFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalPathExpand pathExpand = call.rel(0);
            GraphLogicalGetV getV = (GraphLogicalGetV) pathExpand.getGetV();
            GraphLogicalExpand expand = (GraphLogicalExpand) pathExpand.getExpand();
            RelNode fused = transform(getV, expand, null);
            GraphLogicalPathExpand afterPathExpand =
                    GraphLogicalPathExpand.create(
                            (GraphOptCluster) pathExpand.getCluster(),
                            ImmutableList.of(),
                            pathExpand.getInput(0),
                            fused,
                            pathExpand.getOffset(),
                            pathExpand.getFetch(),
                            pathExpand.getResultOpt(),
                            pathExpand.getPathOpt(),
                            pathExpand.getUntilCondition(),
                            pathExpand.getAliasName(),
                            pathExpand.getStartAlias(),
                            pathExpand.isOptional());
            if (pathExpand.getCachedCost() != null) {
                afterPathExpand.setCachedCost(pathExpand.getCachedCost());
            }
            call.transformTo(afterPathExpand);
        }

        public static class Config implements RelRule.Config {
            public static PathBaseExpandGetVFusionRule.Config DEFAULT =
                    new Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalPathExpand.class)
                                                    .predicate(
                                                            (GraphLogicalPathExpand pathExpand) -> {
                                                                if (GraphOpt.PathExpandResult
                                                                        .ALL_V_E
                                                                        .equals(
                                                                                pathExpand
                                                                                        .getResultOpt())) {
                                                                    return false;
                                                                } else {
                                                                    if (pathExpand.getExpand()
                                                                            instanceof
                                                                            GraphLogicalExpand) {
                                                                        GraphLogicalExpand expand =
                                                                                (GraphLogicalExpand)
                                                                                        pathExpand
                                                                                                .getExpand();
                                                                        int alias =
                                                                                expand.getAliasId();
                                                                        return alias
                                                                                == AliasInference
                                                                                        .DEFAULT_ID;
                                                                    } else {
                                                                        return false;
                                                                    }
                                                                }
                                                            })
                                                    .anyInputs());

            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public RelRule toRule() {
                return new PathBaseExpandGetVFusionRule(this);
            }

            @Override
            public Config withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public Config withOperandSupplier(OperandTransform operandTransform) {
                this.operandSupplier = operandTransform;
                return this;
            }

            @Override
            public OperandTransform operandSupplier() {
                return this.operandSupplier;
            }

            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable String description() {
                return this.description;
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }
}

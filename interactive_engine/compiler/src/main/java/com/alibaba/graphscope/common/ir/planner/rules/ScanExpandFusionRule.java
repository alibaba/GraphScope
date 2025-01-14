/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This rule transforms edge expansion into edge scan wherever possible.
 * For example, consider the following Cypher query:
 * Match (a:PERSON)-[b:KNOWS]->(c:PERSON) Return b.name;
 *
 * Although the query involves Scan and GetV steps, their results are not directly utilized by subsequent
 * project operations. The only effectively used data is the edge data produced by the Expand operation.
 * In such cases, we can perform a fusion operation, transforming the pattern
 * (a:PERSON)-[b:KNOWS]->(c:PERSON) into a scan operation on the KNOWS edge.
 *
 * It is important to note that whether fusion is feasible also depends on the label dependencies between
 * nodes and edges. If the edge label is determined strictly by the triplet (src_label, edge_label, dst_label),
 * fusion cannot be performed. For reference, consider the following query:
 * Match (a:PERSON)-[b:LIKES]->(c:COMMENT) Return b.name;
 */
public class ScanExpandFusionRule extends RelRule {

    protected ScanExpandFusionRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        GraphLogicalProject project = relOptRuleCall.rel(0);
        GraphLogicalGetV getV = relOptRuleCall.rel(1);
        GraphLogicalExpand expand = relOptRuleCall.rel(2);
        GraphLogicalSource source = relOptRuleCall.rel(3);
        if (!checkLabel(getV, expand, source)
                || !checkOpt(getV, expand, source)
                || !checkAlias(project, getV, expand, source)
                || !checkFilters(getV, expand, source)) {
            return;
        }
        GraphLogicalSource fused =
                GraphLogicalSource.create(
                        (GraphOptCluster) expand.getCluster(),
                        expand.getHints(),
                        GraphOpt.Source.EDGE,
                        expand.getTableConfig(),
                        expand.getAliasName(),
                        source.getParams(),
                        source.getUniqueKeyFilters(),
                        expand.getFilters());
        RelNode newProject = project.copy(project.getTraitSet(), ImmutableList.of(fused));
        relOptRuleCall.transformTo(newProject);
    }

    private boolean checkOpt(
            GraphLogicalGetV getV, GraphLogicalExpand expand, GraphLogicalSource source) {
        return source.getOpt() == GraphOpt.Source.VERTEX
                && (expand.getOpt() == GraphOpt.Expand.OUT && getV.getOpt() == GraphOpt.GetV.END
                        || expand.getOpt() == GraphOpt.Expand.IN
                                && getV.getOpt() == GraphOpt.GetV.START
                        || expand.getOpt() == GraphOpt.Expand.BOTH
                                && getV.getOpt() == GraphOpt.GetV.BOTH);
    }

    private boolean checkAlias(
            GraphLogicalProject project,
            GraphLogicalGetV getV,
            GraphLogicalExpand expand,
            GraphLogicalSource source) {
        List<Integer> usedAliasIds = Lists.newArrayList();
        RexVariableAliasCollector<Integer> collector =
                new RexVariableAliasCollector<>(true, var -> var.getAliasId());
        project.getProjects()
                .forEach(
                        expr -> {
                            usedAliasIds.addAll(expr.accept(collector));
                        });
        if (source.getAliasId() != AliasInference.DEFAULT_ID
                && usedAliasIds.contains(source.getAliasId())) {
            return false;
        }
        if (getV.getAliasId() != AliasInference.DEFAULT_ID
                && usedAliasIds.contains(getV.getAliasId())) {
            return false;
        }
        return true;
    }

    private boolean checkLabel(
            GraphLogicalGetV getV, GraphLogicalExpand expand, GraphLogicalSource source) {
        GraphLabelType sourceType = Utils.getGraphLabels(source.getRowType());
        GraphLabelType getVType = Utils.getGraphLabels(getV.getRowType());
        List<String> sourceCandidates =
                sourceType.getLabelsEntry().stream()
                        .map(k -> k.getLabel())
                        .collect(Collectors.toList());
        List<String> getVCandidates =
                getVType.getLabelsEntry().stream()
                        .map(k -> k.getLabel())
                        .collect(Collectors.toList());
        List<RelOptTable> optTables = expand.getTableConfig().getTables();
        for (RelOptTable optTable : optTables) {
            GraphLabelType expandType = Utils.getGraphLabels(optTable.getRowType());
            for (GraphLabelType.Entry entry : expandType.getLabelsEntry()) {
                switch (expand.getOpt()) {
                    case OUT:
                        if (!sourceCandidates.contains(entry.getSrcLabel())
                                || !getVCandidates.contains(entry.getDstLabel())) {
                            return false;
                        }
                        break;
                    case IN:
                        if (!sourceCandidates.contains(entry.getDstLabel())
                                || !getVCandidates.contains(entry.getSrcLabel())) {
                            return false;
                        }
                        break;
                    case BOTH:
                    default:
                        if ((!sourceCandidates.contains(entry.getSrcLabel())
                                        || !getVCandidates.contains(entry.getDstLabel()))
                                && (!sourceCandidates.contains(entry.getDstLabel())
                                        || !getVCandidates.contains(entry.getSrcLabel()))) {
                            return false;
                        }
                }
            }
        }
        return true;
    }

    private boolean checkFilters(
            GraphLogicalGetV getV, GraphLogicalExpand expand, GraphLogicalSource source) {
        return source.getUniqueKeyFilters() == null
                && source.getParams().getParams().isEmpty()
                && ObjectUtils.isEmpty(source.getFilters())
                && ObjectUtils.isEmpty(getV.getFilters());
    }

    public static class Config implements RelRule.Config {
        public static RelRule.Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(GraphLogicalProject.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(GraphLogicalGetV.class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        GraphLogicalExpand
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        b3 ->
                                                                                                                b3.operand(
                                                                                                                                GraphLogicalSource
                                                                                                                                        .class)
                                                                                                                        .anyInputs()))));

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;

        @Override
        public ScanExpandFusionRule toRule() {
            return new ScanExpandFusionRule(this);
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
        public Config withOperandSupplier(RelRule.OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        @Override
        public RelRule.OperandTransform operandSupplier() {
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

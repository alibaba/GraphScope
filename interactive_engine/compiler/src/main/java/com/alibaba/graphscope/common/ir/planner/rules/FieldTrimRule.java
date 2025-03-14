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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * The {@code FieldTrimRule} trims the fields of a relational node based on the fields used by its parent node.
 * We implement this rule specifically, rather than extending Calcite's rule framework, to allow passing intermediate parameters during recursive invocations.
 * By extending {@code RelFieldTrimmer}, we maintain flexibility for customized behavior.
 */
public abstract class FieldTrimRule {
    public static RelNode trim(GraphBuilder builder, RelNode rel, PlannerConfig config) {
        GraphFieldTrimmer trimmer = new GraphFieldTrimmer(builder, config);
        return trimmer.trim(rel);
    }

    public static class GraphFieldTrimmer extends RelFieldTrimmer {
        private final GraphBuilder builder;
        private final PlannerConfig config;
        private final IdentityHashMap<CommonTableScan, TrimResult> trimmedCommon;

        public GraphFieldTrimmer(GraphBuilder builder, PlannerConfig config) {
            super(null, builder);
            this.builder = builder;
            this.config = config;
            this.trimmedCommon = new IdentityHashMap<>();
        }

        private boolean trimClass(Class<? extends RelNode> relClass) {
            return config.getTrimClassNames().contains(relClass.getSimpleName());
        }

        @Override
        public RelNode trim(RelNode root) {
            ImmutableBitSet.Builder fieldsUsed = ImmutableBitSet.builder();
            root.getRowType()
                    .getFieldList()
                    .forEach(
                            k -> {
                                fieldsUsed.set(k.getIndex());
                            });
            TrimResult trimResult =
                    this.dispatchTrimFields(root, fieldsUsed.build(), Collections.emptySet());
            Mapping mapping = trimResult.right;
            if (mapping.getTargetCount() != mapping.getSourceCount()) {
                throw new IllegalArgumentException(
                        "trim should not change the final output fields");
            }
            return trimResult.left;
        }

        /**
         * Trims fields for {@code GraphLogicalSource} by resetting the alias name or ID to their default values.
         *
         * @param source      the {@code GraphLogicalSource} instance to process
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after trimming
         */
        public TrimResult trimFields(
                GraphLogicalSource source,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            int aliasId = source.getAliasId();
            if (!trimClass(source.getClass())
                    || aliasId == AliasInference.DEFAULT_ID
                    || fieldsUsed.get(aliasId)) {
                return result(source);
            }
            RelNode newSource =
                    GraphLogicalSource.create(
                            (GraphOptCluster) source.getCluster(),
                            source.getHints(),
                            source.getOpt(),
                            source.getTableConfig(),
                            AliasInference.DEFAULT_NAME,
                            source.getParams(),
                            source.getUniqueKeyFilters(),
                            source.getFilters());
            return result(newSource, source);
        }

        /**
         * Trims fields for {@code GraphLogicalExpand} by resetting the alias name or ID to default values.
         * While performing the trim, the fields in use are preserved. If the source starts from a specific alias instead of the input,
         * the alias ID is added to the used fields to ensure that nodes marked with a start tag are not removed during the trimming process.
         *
         * @param expand      the {@code GraphLogicalExpand} instance to process
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after trimming
         */
        public TrimResult trimFields(
                GraphLogicalExpand expand,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            boolean startFromInput = startFromInput(expand.getInput(0), expand.getStartAlias());
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            if (!startFromInput) {
                inputFieldsUsed.set(expand.getStartAlias().getAliasId());
            }
            TrimResult trimChild =
                    trimChild(expand, expand.getInput(0), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            AliasNameWithId newStartAlias =
                    getNewStartAlias(startFromInput, newInput, expand.getStartAlias());
            if (!trimClass(expand.getClass())
                    && newInput == expand.getInput(0)
                    && newStartAlias == expand.getStartAlias()) {
                return result(expand);
            }
            String newAliasName =
                    getNewAliasName(
                            expand,
                            new AliasNameWithId(expand.getAliasName(), expand.getAliasId()),
                            fieldsUsed);
            if (newInput == expand.getInput(0)
                    && newStartAlias == expand.getStartAlias()
                    && newAliasName == expand.getAliasName()) {
                return result(expand);
            }
            RelNode newExpand =
                    GraphLogicalExpand.create(
                            (GraphOptCluster) expand.getCluster(),
                            expand.getHints(),
                            newInput,
                            expand.getOpt(),
                            expand.getTableConfig(),
                            newAliasName,
                            newStartAlias,
                            expand.isOptional(),
                            expand.getFilters(),
                            (GraphSchemaType) expand.getRowType().getFieldList().get(0).getType());
            return result(newExpand, expand);
        }

        private AliasNameWithId getNewStartAlias(
                boolean startFromInput, RelNode newInput, AliasNameWithId oldStartAlias) {
            AliasNameWithId newStartAlias = oldStartAlias;
            if (startFromInput
                    && !oldStartAlias.equals(AliasNameWithId.DEFAULT)
                    && Utils.getAlias(newInput) == AliasInference.DEFAULT_NAME) {
                newStartAlias = AliasNameWithId.DEFAULT;
            }
            return newStartAlias;
        }

        private String getNewAliasName(
                RelNode rel, AliasNameWithId oldAlias, ImmutableBitSet fieldsUsed) {
            String oldAliasName = oldAlias.getAliasName();
            if (!trimClass(rel.getClass())) {
                return oldAliasName;
            }
            if (oldAlias.getAliasId() != AliasInference.DEFAULT_ID
                    && !fieldsUsed.get(oldAlias.getAliasId())) {
                return AliasInference.DEFAULT_NAME;
            }
            return oldAliasName;
        }

        public TrimResult trimFields(
                GraphLogicalGetV getV,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            boolean startFromInput = startFromInput(getV.getInput(0), getV.getStartAlias());
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            if (!startFromInput) {
                inputFieldsUsed.set(getV.getStartAlias().getAliasId());
            }
            TrimResult trimChild =
                    trimChild(getV, getV.getInput(0), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            AliasNameWithId newStartAlias =
                    getNewStartAlias(startFromInput, newInput, getV.getStartAlias());
            if (!trimClass(getV.getClass())
                    && newInput == getV.getInput(0)
                    && newStartAlias == getV.getStartAlias()) {
                return result(getV);
            }
            String newAliasName =
                    getNewAliasName(
                            getV,
                            new AliasNameWithId(getV.getAliasName(), getV.getAliasId()),
                            fieldsUsed);
            if (newInput == getV.getInput(0)
                    && newStartAlias == getV.getStartAlias()
                    && newAliasName == getV.getAliasName()) {
                return result(getV);
            }
            RelNode newGetV =
                    GraphLogicalGetV.create(
                            (GraphOptCluster) getV.getCluster(),
                            getV.getHints(),
                            newInput,
                            getV.getOpt(),
                            getV.getTableConfig(),
                            newAliasName,
                            newStartAlias,
                            getV.getFilters());
            return result(newGetV, getV);
        }

        public TrimResult trimFields(
                GraphLogicalPathExpand pxd,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            boolean startFromInput = startFromInput(pxd.getInput(), pxd.getStartAlias());
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            if (!startFromInput) {
                inputFieldsUsed.set(pxd.getStartAlias().getAliasId());
            }
            TrimResult trimChild =
                    trimChild(pxd, pxd.getInput(0), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            AliasNameWithId newStartAlias =
                    getNewStartAlias(startFromInput, newInput, pxd.getStartAlias());
            if (!trimClass(pxd.getClass())
                    && newInput == pxd.getInput(0)
                    && newStartAlias == pxd.getStartAlias()) {
                return result(pxd);
            }
            String newAliasName =
                    getNewAliasName(
                            pxd,
                            new AliasNameWithId(pxd.getAliasName(), pxd.getAliasId()),
                            fieldsUsed);
            if (newInput == pxd.getInput(0)
                    && newStartAlias == pxd.getStartAlias()
                    && newAliasName == pxd.getAliasName()) {
                return result(pxd);
            }
            RelNode newPxd;
            if (pxd.getFused() != null) {
                newPxd =
                        GraphLogicalPathExpand.create(
                                (GraphOptCluster) pxd.getCluster(),
                                ImmutableList.of(),
                                newInput,
                                pxd.getFused(),
                                pxd.getOffset(),
                                pxd.getFetch(),
                                pxd.getResultOpt(),
                                pxd.getPathOpt(),
                                pxd.getUntilCondition(),
                                newAliasName,
                                newStartAlias,
                                pxd.isOptional());
            } else {
                newPxd =
                        GraphLogicalPathExpand.create(
                                (GraphOptCluster) pxd.getCluster(),
                                ImmutableList.of(),
                                newInput,
                                pxd.getExpand(),
                                pxd.getGetV(),
                                pxd.getOffset(),
                                pxd.getFetch(),
                                pxd.getResultOpt(),
                                pxd.getPathOpt(),
                                pxd.getUntilCondition(),
                                newAliasName,
                                newStartAlias,
                                pxd.isOptional());
            }
            return result(newPxd, pxd);
        }

        /**
         * The FieldTrimRule is applied before the ExpandGetVFusionRule, which is the sole mechanism for generating a {@code GraphPhysicalExpand}.
         * An exception is thrown here because encountering a {@code GraphPhysicalExpand} at this stage is unexpected.
         *
         * @param expand      the {@code GraphPhysicalExpand} instance to process
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after trimming
         */
        public TrimResult trimFields(
                GraphPhysicalExpand expand,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            throw new UnsupportedOperationException(
                    "trim fields for physical expand is unsupported yet");
        }

        public TrimResult trimFields(
                GraphPhysicalGetV getV,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            throw new UnsupportedOperationException(
                    "trim fields for physical getV is unsupported yet");
        }

        /**
         * Maintains the set of used fields based on the conditions applied in the filter.
         *
         * @param filter      the filter condition being processed
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after applying the filter
         */
        public TrimResult trimFields(
                LogicalFilter filter,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            List<Integer> conditionFields =
                    filter.getCondition()
                            .accept(
                                    new RexVariableAliasCollector<>(
                                            true, (RexGraphVariable var) -> var.getAliasId()));
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild().addAll(conditionFields);
            TrimResult trimChild =
                    trimChild(filter, filter.getInput(), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == filter.getInput()) {
                return result(filter);
            }
            return result(filter.copy(filter.getTraitSet(), ImmutableList.of(newInput)), filter);
        }

        /**
         * Trims project fields if they are not used by the parent node, and preserves the used fields for the input only if the project fields are retained.
         *
         * @param project     the project operation being processed
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after trimming
         */
        public TrimResult trimFields(
                GraphLogicalProject project,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
            List<RelDataTypeField> newFields = Lists.newArrayList();
            List<RexNode> newProjects = Lists.newArrayList();
            boolean trimProject = trimClass(project.getClass());
            for (Ord<RelDataTypeField> field : Ord.zip(project.getRowType().getFieldList())) {
                if (!trimProject || fieldsUsed.get(field.e.getIndex())) {
                    RexNode expr = project.getProjects().get(field.i);
                    List<Integer> exprFields =
                            expr.accept(
                                    new RexVariableAliasCollector<>(
                                            true, (RexGraphVariable var) -> var.getAliasId()));
                    inputFieldsUsed.addAll(exprFields);
                    newFields.add(field.e);
                    newProjects.add(expr);
                }
            }
            TrimResult trimChild =
                    trimChild(
                            project,
                            project.getInput(),
                            project.isAppend()
                                    ? inputFieldsUsed.addAll(fieldsUsed).build()
                                    : inputFieldsUsed.build(),
                            extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == project.getInput()
                    && newFields.size() == project.getRowType().getFieldCount()) {
                return result(project);
            }
            RelNode newProject =
                    GraphLogicalProject.create(
                            (GraphOptCluster) project.getCluster(),
                            project.getHints(),
                            newInput,
                            newProjects,
                            new RelRecordType(newFields),
                            project.isAppend());
            return result(newProject, project);
        }

        /**
         * Maintains the used fields for each group key and value field to trim the input node accordingly.
         *
         * @param aggregate   the aggregate operation being processed
         * @param fieldsUsed  the set of fields currently in use
         * @param extraFields any additional fields required during processing
         * @return            the updated set of fields after trimming
         */
        public TrimResult trimFields(
                GraphLogicalAggregate aggregate,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
            aggregate
                    .getGroupKey()
                    .getVariables()
                    .forEach(
                            k1 -> {
                                List<Integer> keyFields =
                                        k1.accept(
                                                new RexVariableAliasCollector<>(
                                                        true,
                                                        (RexGraphVariable var) ->
                                                                var.getAliasId()));
                                inputFieldsUsed.addAll(keyFields);
                            });
            aggregate
                    .getAggCalls()
                    .forEach(
                            k2 -> {
                                k2.getOperands()
                                        .forEach(
                                                k3 -> {
                                                    List<Integer> valueFields =
                                                            k3.accept(
                                                                    new RexVariableAliasCollector<>(
                                                                            true,
                                                                            (RexGraphVariable
                                                                                            var) ->
                                                                                    var
                                                                                            .getAliasId()));
                                                    inputFieldsUsed.addAll(valueFields);
                                                });
                            });
            TrimResult trimChild =
                    trimChild(
                            aggregate, aggregate.getInput(), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == aggregate.getInput()) {
                return result(aggregate);
            }
            return result(
                    aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(newInput)), aggregate);
        }

        public TrimResult trimFields(
                GraphLogicalSort sort,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            sort.getSortExps()
                    .forEach(
                            expr ->
                                    inputFieldsUsed.addAll(
                                            expr.accept(
                                                    new RexVariableAliasCollector<>(
                                                            true,
                                                            (RexGraphVariable var) ->
                                                                    var.getAliasId()))));
            TrimResult trimChild =
                    trimChild(sort, sort.getInput(), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == sort.getInput()) {
                return result(sort);
            }
            return result(sort.copy(sort.getTraitSet(), ImmutableList.of(newInput)), sort);
        }

        public TrimResult trimFields(
                GraphLogicalUnfold unfold,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            inputFieldsUsed.addAll(
                    unfold.getUnfoldKey()
                            .accept(
                                    new RexVariableAliasCollector<>(
                                            true, (RexGraphVariable var) -> var.getAliasId())));
            TrimResult trimChild =
                    trimChild(unfold, unfold.getInput(), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == unfold.getInput()) {
                return result(unfold);
            }
            return result(unfold.copy(unfold.getTraitSet(), ImmutableList.of(newInput)), unfold);
        }

        public TrimResult trimFields(
                GraphLogicalDedupBy dedupBy,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            dedupBy.getDedupByKeys()
                    .forEach(
                            k ->
                                    inputFieldsUsed.addAll(
                                            k.accept(
                                                    new RexVariableAliasCollector<>(
                                                            true,
                                                            (RexGraphVariable var) ->
                                                                    var.getAliasId()))));
            TrimResult trimChild =
                    trimChild(dedupBy, dedupBy.getInput(), inputFieldsUsed.build(), extraFields);
            RelNode newInput = trimChild.left;
            if (newInput == dedupBy.getInput()) {
                return result(dedupBy);
            }
            return result(dedupBy.copy(dedupBy.getTraitSet(), ImmutableList.of(newInput)), dedupBy);
        }

        public TrimResult trimFields(
                LogicalJoin join, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            join.getCondition()
                    .accept(
                            new RexVariableAliasCollector<>(
                                    true, (RexGraphVariable var) -> var.getAliasId()))
                    .forEach(k -> inputFieldsUsed.set(k));
            int changeCount = 0;
            List<RelNode> newInputs = Lists.newArrayList();
            ImmutableBitSet inputFields = inputFieldsUsed.build();
            for (Ord<RelNode> input : Ord.zip(join.getInputs())) {
                TrimResult trimChild = trimChild(join, input.e, inputFields, extraFields);
                RelNode newInput = trimChild.left;
                if (newInput != input.e) {
                    changeCount++;
                }
                newInputs.add(newInput);
            }
            if (changeCount == 0) {
                return result(join);
            }
            return result(join.copy(join.getTraitSet(), newInputs), join);
        }

        public TrimResult trimFields(
                MultiJoin join, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
            ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
            join.getJoinFilter()
                    .accept(
                            new RexVariableAliasCollector<>(
                                    true, (RexGraphVariable var) -> var.getAliasId()))
                    .forEach(k -> inputFieldsUsed.set(k));
            int changeCount = 0;
            List<RelNode> newInputs = Lists.newArrayList();
            ImmutableBitSet inputFields = inputFieldsUsed.build();
            for (RelNode input : join.getInputs()) {
                TrimResult trimChild = trimChild(join, input, inputFields, extraFields);
                RelNode newInput = trimChild.left;
                if (newInput != input) {
                    changeCount++;
                }
                newInputs.add(newInput);
            }
            if (changeCount == 0) {
                return result(join);
            }
            return result(join.copy(join.getTraitSet(), newInputs), join);
        }

        public TrimResult trimFields(
                CommonTableScan tableScan,
                ImmutableBitSet fieldsUsed,
                Set<RelDataTypeField> extraFields) {
            TrimResult trimmed = trimmedCommon.get(tableScan);
            if (trimmed != null) {
                return trimmed;
            }
            RelNode common = ((CommonOptTable) tableScan.getTable()).getCommon();
            TrimResult trimCommon = dispatchTrimFields(common, fieldsUsed, extraFields);
            RelNode newCommon = trimCommon.left;
            TrimResult result;
            if (newCommon == common) {
                result =
                        result(
                                tableScan,
                                Mappings.createIdentity(tableScan.getRowType().getFieldCount()));
            } else {
                RelNode newTableScan =
                        new CommonTableScan(
                                tableScan.getCluster(),
                                tableScan.getTraitSet(),
                                new CommonOptTable(newCommon));
                result = result(newTableScan, tableScan);
            }
            trimmedCommon.put(tableScan, result);
            return result;
        }

        public TrimResult trimFields(
                LogicalUnion union, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
            int changeCount = 0;
            List<RelNode> newInputs = Lists.newArrayList();
            for (RelNode input : union.getInputs()) {
                TrimResult trimChild = trimChild(union, input, fieldsUsed, extraFields);
                RelNode newInput = trimChild.left;
                if (newInput != input) {
                    changeCount++;
                }
                newInputs.add(newInput);
            }
            if (changeCount == 0) {
                return result(union);
            }
            return result(union.copy(union.getTraitSet(), newInputs), union);
        }

        private boolean startFromInput(RelNode input, AliasNameWithId startAlias) {
            if (startAlias.getAliasName() == AliasInference.DEFAULT_NAME) return true;
            String alias = Utils.getAlias(input);
            return alias == null ? false : alias.equals(startAlias.getAliasName());
        }

        private TrimResult result(RelNode after) {
            return result(after, after);
        }

        private TrimResult result(RelNode after, RelNode before) {
            if (after == before) {
                return super.result(
                        after, Mappings.createIdentity(after.getRowType().getFieldCount()));
            }
            // The trim operation changes the column IDs but not the alias IDs.
            // todo: This mapping is maintained to track the relationship between the column IDs
            // before
            // and after trimming.
            return super.result(
                    after,
                    Mappings.create(
                            MappingType.INVERSE_SURJECTION,
                            before.getRowType().getFieldCount(),
                            after.getRowType().getFieldCount()),
                    before);
        }
    }
}

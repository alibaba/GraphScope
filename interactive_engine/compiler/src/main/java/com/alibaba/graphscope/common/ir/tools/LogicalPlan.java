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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.DummyTableScan;
import com.alibaba.graphscope.common.ir.rel.ddl.GraphTableModify;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.rex.RexProcedureCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * logical plan for a query which can be a regular query or a procedure call
 */
public class LogicalPlan {
    private @Nullable RelNode regularQuery;
    private @Nullable RexNode procedureCall;
    private final Mode mode;

    private final List<StoredProcedureMeta.Parameter> dynamicParams;

    public LogicalPlan(RelNode regularQuery) {
        this(regularQuery, ImmutableList.of());
    }

    public LogicalPlan(RelNode regularQuery, List<StoredProcedureMeta.Parameter> dynamicParams) {
        this.regularQuery = Objects.requireNonNull(regularQuery);
        this.dynamicParams = Objects.requireNonNull(dynamicParams);
        this.mode = analyzeMode(regularQuery);
    }

    public LogicalPlan(RexNode procedureCall) {
        this.procedureCall = Objects.requireNonNull(procedureCall);
        this.dynamicParams = ImmutableList.of();
        if (procedureCall instanceof RexProcedureCall
                && ((RexProcedureCall) procedureCall).getMode()
                        == StoredProcedureMeta.Mode.SCHEMA) {
            this.mode = Mode.SCHEMA;
        } else {
            this.mode = Mode.PROCEDURE;
        }
    }

    public @Nullable RelNode getRegularQuery() {
        return regularQuery;
    }

    public @Nullable RexNode getProcedureCall() {
        return procedureCall;
    }

    public Mode getMode() {
        return this.mode;
    }

    public boolean isReturnEmpty() {
        return this.mode == Mode.EMPTY;
    }

    public String explain() {
        if (this.regularQuery != null) {
            return this.regularQuery.explain();
        } else if (this.procedureCall != null) {
            return this.procedureCall.toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    private Mode analyzeMode(RelNode regularQuery) {
        List<RelNode> inputs = Lists.newArrayList(regularQuery);
        Mode mode = Mode.WRITE_ONLY;
        while (!inputs.isEmpty()) {
            RelNode cur = inputs.remove(0);
            if (cur instanceof LogicalValues) {
                return Mode.EMPTY;
            }
            if (cur instanceof GraphLogicalSingleMatch) {
                GraphLogicalSingleMatch match = (GraphLogicalSingleMatch) cur;
                inputs.add(match.getSentence());
            } else if (cur instanceof GraphLogicalMultiMatch) {
                GraphLogicalMultiMatch match = (GraphLogicalMultiMatch) cur;
                for (RelNode sentence : match.getSentences()) {
                    inputs.add(sentence);
                }
            } else if (cur instanceof CommonTableScan) {
                inputs.add(((CommonOptTable) ((CommonTableScan) cur).getTable()).getCommon());
            } else if (cur instanceof DummyTableScan) {
                // do nothing
            } else {
                if (!writeOnly(cur)) {
                    mode = Mode.READ_WRITE;
                }
                inputs.addAll(cur.getInputs());
            }
        }
        return mode;
    }

    private boolean writeOnly(RelNode rel) {
        if (rel instanceof GraphTableModify.Insert) {
            TargetGraph graph = ((GraphTableModify.Insert) rel).getTargetGraph();
            FieldMappings mappings = graph.getMappings();
            return writeOnly(mappings);
        }
        if (rel instanceof GraphTableModify.Update) {
            FieldMappings mappings = ((GraphTableModify.Update) rel).getUpdateMappings();
            return writeOnly(mappings);
        }
        if (rel instanceof GraphTableModify.Delete) {
            return true;
        }
        return false;
    }

    private boolean writeOnly(FieldMappings mappings) {
        return mappings.getMappings().stream()
                .allMatch(entry -> entry.getSource() instanceof RexLiteral);
    }

    public @Nullable RelDataType getOutputType() {
        if (regularQuery != null) {
            return Utils.getOutputType(regularQuery);
        } else if (procedureCall != null) {
            return procedureCall.getType();
        } else {
            return null;
        }
    }

    public List<StoredProcedureMeta.Parameter> getDynamicParams() {
        return Collections.unmodifiableList(dynamicParams);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicalPlan that = (LogicalPlan) o;
        return Objects.equals(getDigest(regularQuery), getDigest(that.regularQuery))
                && Objects.equals(procedureCall, that.procedureCall)
                && Objects.equals(dynamicParams, that.dynamicParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDigest(regularQuery), procedureCall, dynamicParams);
    }

    private String getDigest(RelNode rel) {
        return rel == null ? null : rel.explain();
    }

    public enum Mode {
        SCHEMA,
        PROCEDURE,
        WRITE_ONLY,
        READ_WRITE,
        EMPTY,
    }
}

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

package com.alibaba.graphscope.common.ir.runtime.ffi;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalNode;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.stream.Collectors;

public class FfiLogicalPlan extends LogicalPlan<Pointer, byte[]> {
    private static final IrCoreLibrary LIB = IrCoreLibrary.INSTANCE;

    private final Pointer ptrPlan;

    private int lastIdx;

    public FfiLogicalPlan(RelOptCluster cluster, IrMeta irMeta, List<RelHint> hints) {
        super(cluster, hints);
        checkFfiResult(LIB.setSchema(irMeta.getSchemaJson()));
        this.ptrPlan = LIB.initLogicalPlan();
        this.lastIdx = -1;
    }

    @Override
    public void appendNode(LogicalNode<Pointer> node) {
        IntByReference oprIdx = new IntByReference();
        RelNode original = node.getOriginal();
        if (original instanceof GraphLogicalSource) {
            checkFfiResult(LIB.appendScanOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalExpand) {
            checkFfiResult(LIB.appendEdgexpdOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalGetV) {
            checkFfiResult(LIB.appendGetvOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalPathExpand) {
            checkFfiResult(LIB.appendPathxpdOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalSingleMatch
                || original instanceof GraphLogicalMultiMatch) {
            appendDummySource(GraphOpt.Source.VERTEX);
            checkFfiResult(LIB.appendPatternOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalProject) {
            checkFfiResult(LIB.appendProjectOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof LogicalFilter) {
            checkFfiResult(LIB.appendSelectOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalAggregate) {
            // transform aggregate to project + dedup by key
            if (((GraphLogicalAggregate) original).getAggCalls().isEmpty()) {
                appendProjectDedup((GraphLogicalAggregate) original);
            } else {
                checkFfiResult(LIB.appendGroupbyOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
            }
        } else if (original instanceof GraphLogicalSort) {
            if (((GraphLogicalSort) original).getCollation().getFieldCollations().isEmpty()) {
                checkFfiResult(LIB.appendLimitOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
            } else {
                checkFfiResult(LIB.appendOrderbyOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
            }
        } else {
            throw new UnsupportedOperationException(
                    "node type " + original.getClass() + " can not be appended to the ffi plan");
        }
        this.lastIdx = oprIdx.getValue();
    }

    private void appendDummySource(GraphOpt.Source opt) {
        IntByReference oprIdx = new IntByReference();
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(opt));
        checkFfiResult(LIB.appendScanOperator(ptrPlan, ptrScan, lastIdx, oprIdx));
        this.lastIdx = oprIdx.getValue();
    }

    private void appendSink() {
        Pointer ptrSink = LIB.initSinkOperator();
        checkFfiResult(LIB.appendSinkOperator(ptrPlan, ptrSink, lastIdx, new IntByReference()));
    }

    private void appendProjectDedup(GraphLogicalAggregate aggregate) {
        GraphGroupKeys keys = aggregate.getGroupKey();
        Preconditions.checkArgument(
                keys.groupKeyCount() > 0 && aggregate.getAggCalls().isEmpty(),
                "group keys should not be empty while group calls should be empty if need dedup");
        List<OuterExpression.Expression> exprs =
                keys.getVariables().stream()
                        .map(
                                k -> {
                                    Preconditions.checkArgument(
                                            k instanceof RexGraphVariable,
                                            "each group key should be type %s, but is %s",
                                            RexGraphVariable.class,
                                            k.getClass());
                                    return k.accept(new RexToProtoConverter(true, isColumnId()));
                                })
                        .collect(Collectors.toList());
        List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
        Pointer ptrProject = LIB.initProjectOperator(false);
        for (int i = 0; i < exprs.size(); ++i) {
            OuterExpression.Expression expr = exprs.get(i);
            int aliasId;
            if (i >= fields.size()
                    || (aliasId = fields.get(i).getIndex()) == AliasInference.DEFAULT_ID) {
                throw new IllegalArgumentException(
                        "each group key should have an alias if need dedup");
            }
            checkFfiResult(
                    LIB.addProjectExprAliasWithPb(
                            ptrProject,
                            new FfiPbPointer.ByValue(expr.toByteArray()),
                            ArgUtils.asAlias(aliasId)));
        }
        Pointer ptrDedup = LIB.initDedupOperator();
        exprs.forEach(
                k -> {
                    OuterExpression.Variable var = k.getOperators(0).getVar();
                    checkFfiResult(
                            LIB.addDedupKeyWithPb(
                                    ptrDedup, new FfiPbPointer.ByValue(var.toByteArray())));
                });
        IntByReference oprIdx = new IntByReference();
        checkFfiResult(LIB.appendProjectOperator(ptrPlan, ptrProject, lastIdx, oprIdx));
        this.lastIdx = oprIdx.getValue();
        checkFfiResult(LIB.appendDedupOperator(ptrPlan, ptrDedup, lastIdx, oprIdx));
        this.lastIdx = oprIdx.getValue();
    }

    @Override
    public String explain() {
        FfiResult res = LIB.printPlanAsJson(this.ptrPlan);
        if (res == null || res.code != ResultCode.Success) {
            throw new IllegalStateException("print plan in ir core fail, msg : %s" + res, null);
        }
        return res.msg;
    }

    @Override
    public byte[] toPhysical() {
        appendSink();
        int servers = Integer.valueOf(hints.get(0).kvOptions.get("servers"));
        int workers = Integer.valueOf(hints.get(0).kvOptions.get("workers"));
        FfiData.ByValue ffiData = LIB.buildPhysicalPlan(ptrPlan, workers, servers);
        checkFfiResult(ffiData.error);
        byte[] bytes = ffiData.getBytes();
        ffiData.close();
        return bytes;
    }

    @Override
    public void close() throws Exception {
        if (this.ptrPlan != null) {
            LIB.destroyLogicalPlan(this.ptrPlan);
        }
    }

    private void checkFfiResult(FfiResult res) {
        if (res == null || res.code != ResultCode.Success) {
            throw new IllegalStateException(
                    "build logical plan, unexpected ffi results from ir_core, msg : " + res);
        }
    }

    private boolean isColumnId() {
        return Boolean.valueOf(hints.get(0).kvOptions.get("isColumnId"));
    }
}

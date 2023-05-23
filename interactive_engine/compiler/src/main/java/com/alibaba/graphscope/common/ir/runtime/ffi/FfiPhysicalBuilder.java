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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleWrapper;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.RegularPhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiData;
import com.alibaba.graphscope.common.jna.type.FfiPbPointer;
import com.alibaba.graphscope.common.jna.type.FfiResult;
import com.alibaba.graphscope.common.jna.type.ResultCode;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVariable;

import java.util.List;

/**
 * build ffi logical plan in ir core by jna invocation
 */
public class FfiPhysicalBuilder extends RegularPhysicalBuilder<Pointer, byte[]> {
    private static final IrCoreLibrary LIB = IrCoreLibrary.INSTANCE;
    private final IrMeta irMeta;
    private final Configs graphConfig;
    private final Pointer ptrPlan;
    private int lastIdx;

    public FfiPhysicalBuilder(Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan) {
        super(logicalPlan, new GraphRelShuttleWrapper(new RelToFfiConverter(irMeta.getSchema().isColumnId())));
        this.graphConfig = graphConfig;
        this.irMeta = irMeta;
        checkFfiResult(LIB.setSchema(irMeta.getSchema().schemaJson()));
        this.ptrPlan = LIB.initLogicalPlan();
        this.lastIdx = -1;
        initialize();
    }

    @Override
    public void appendNode(PhysicalNode<Pointer> node) {
        IntByReference oprIdx = new IntByReference(this.lastIdx);
        RelNode original = node.getOriginal();
        if (original instanceof GraphLogicalSource) {
            checkFfiResult(
                    LIB.appendScanOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof GraphLogicalExpand) {
            checkFfiResult(
                    LIB.appendEdgexpdOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof GraphLogicalGetV) {
            checkFfiResult(
                    LIB.appendGetvOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof GraphLogicalPathExpand) {
            checkFfiResult(
                    LIB.appendPathxpdOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof GraphLogicalSingleMatch
                || original instanceof GraphLogicalMultiMatch) {
            appendMatch(node, oprIdx);
        } else if (original instanceof GraphLogicalProject) {
            checkFfiResult(
                    LIB.appendProjectOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof LogicalFilter) {
            checkFfiResult(
                    LIB.appendSelectOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
        } else if (original instanceof GraphLogicalAggregate) {
            // transform aggregate to project + dedup by key
            if (((GraphLogicalAggregate) original).getAggCalls().isEmpty()) {
                appendProjectDedup((GraphLogicalAggregate) original, oprIdx);
            } else {
                checkFfiResult(
                        LIB.appendGroupbyOperator(
                                ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
            }
        } else if (original instanceof GraphLogicalSort) {
            if (((GraphLogicalSort) original).getCollation().getFieldCollations().isEmpty()) {
                checkFfiResult(
                        LIB.appendLimitOperator(
                                ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
            } else {
                checkFfiResult(
                        LIB.appendOrderbyOperator(
                                ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
            }
        } else {
            throw new UnsupportedOperationException(
                    "node type " + original.getClass() + " can not be appended to the ffi plan");
        }
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
    public byte[] build() {
        appendSink(new IntByReference(this.lastIdx));
        int servers = PegasusConfig.PEGASUS_HOSTS.get(graphConfig).split(",").length;
        int workers = PegasusConfig.PEGASUS_WORKER_NUM.get(graphConfig);
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
        return this.irMeta.getSchema().isColumnId();
    }

    private void appendMatch(PhysicalNode<Pointer> node, IntByReference oprIdx) {
        // append dummy source
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(GraphOpt.Source.VERTEX));
        checkFfiResult(LIB.appendScanOperator(ptrPlan, ptrScan, oprIdx.getValue(), oprIdx));
        checkFfiResult(
                LIB.appendPatternOperator(ptrPlan, node.getNode(), oprIdx.getValue(), oprIdx));
    }

    private void appendProjectDedup(GraphLogicalAggregate aggregate, IntByReference oprIdx) {
        GraphGroupKeys keys = aggregate.getGroupKey();
        Preconditions.checkArgument(
                keys.groupKeyCount() > 0 && aggregate.getAggCalls().isEmpty(),
                "group keys should not be empty while group calls should be empty if need dedup");
        List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
        Pointer ptrProject = LIB.initProjectOperator(false);
        for (int i = 0; i < keys.groupKeyCount(); ++i) {
            RexNode var = keys.getVariables().get(i);
            Preconditions.checkArgument(
                    var instanceof RexGraphVariable,
                    "each group key should be type %s, but is %s",
                    RexGraphVariable.class,
                    var.getClass());
            OuterExpression.Expression expr =
                    var.accept(new RexToProtoConverter(true, isColumnId()));
            int aliasId;
            if (i >= fields.size()
                    || (aliasId = fields.get(i).getIndex()) == AliasInference.DEFAULT_ID) {
                throw new IllegalArgumentException(
                        "each group key should have an alias if need dedup");
            }
            checkFfiResult(
                    LIB.addProjectExprPbAlias(
                            ptrProject,
                            new FfiPbPointer.ByValue(expr.toByteArray()),
                            ArgUtils.asAlias(aliasId)));
        }
        Pointer ptrDedup = LIB.initDedupOperator();
        for (int i = 0; i < keys.groupKeyCount(); ++i) {
            RelDataTypeField field = fields.get(i);
            RexVariable rexVar =
                    RexGraphVariable.of(field.getIndex(), field.getName(), field.getType());
            OuterExpression.Variable exprVar =
                    rexVar.accept(new RexToProtoConverter(true, isColumnId()))
                            .getOperators(0)
                            .getVar();
            checkFfiResult(
                    LIB.addDedupKeyPb(ptrDedup, new FfiPbPointer.ByValue(exprVar.toByteArray())));
        }
        checkFfiResult(LIB.appendProjectOperator(ptrPlan, ptrProject, oprIdx.getValue(), oprIdx));
        checkFfiResult(LIB.appendDedupOperator(ptrPlan, ptrDedup, oprIdx.getValue(), oprIdx));
    }

    private void appendSink(IntByReference oprIdx) {
        Pointer ptrSink = LIB.initSinkOperator();
        checkFfiResult(LIB.appendSinkOperator(ptrPlan, ptrSink, oprIdx.getValue(), oprIdx));
    }
}

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

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalNode;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiData;
import com.alibaba.graphscope.common.jna.type.FfiResult;
import com.alibaba.graphscope.common.jna.type.ResultCode;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class FfiLogicalPlan extends LogicalPlan<Pointer, FfiData.ByValue> {
    private static final IrCoreLibrary LIB = IrCoreLibrary.INSTANCE;

    private final Pointer ptrPlan;

    private int lastIdx;

    public FfiLogicalPlan(RelOptCluster cluster, List<RelHint> hints) {
        super(cluster, hints);
        this.ptrPlan = LIB.initLogicalPlan();
        this.lastIdx = 0;
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
            appendGlobalSource(GraphOpt.Source.VERTEX);
            checkFfiResult(LIB.appendPatternOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalProject) {
            checkFfiResult(LIB.appendProjectOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof LogicalFilter) {
            checkFfiResult(LIB.appendSelectOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
        } else if (original instanceof GraphLogicalAggregate) {
            checkFfiResult(LIB.appendGroupbyOperator(ptrPlan, node.getNode(), lastIdx, oprIdx));
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

    private void appendGlobalSource(GraphOpt.Source opt) {
        IntByReference oprIdx = new IntByReference();
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(opt));
        checkFfiResult(LIB.appendScanOperator(ptrPlan, ptrScan, lastIdx, oprIdx));
        this.lastIdx = oprIdx.getValue();
    }

    @Override
    public void explain(@Nullable StringBuilder sb) {
        FfiResult res = LIB.printPlanAsJson(this.ptrPlan);
        if (res == null || res.code != ResultCode.Success) {
            throw new IllegalStateException("illegal ffi results " + res, null);
        }
    }

    @Override
    public FfiData.ByValue toPhysical() {
        return null;
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
                    "build logical plan, unexpected ffi results from ir_core, msg : %s" + res);
        }
    }
}

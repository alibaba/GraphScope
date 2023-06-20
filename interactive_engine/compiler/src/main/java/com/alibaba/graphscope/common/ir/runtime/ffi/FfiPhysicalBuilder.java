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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PegasusConfig;
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
import com.alibaba.graphscope.common.ir.runtime.RegularPhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiData;
import com.alibaba.graphscope.common.jna.type.FfiResult;
import com.alibaba.graphscope.common.jna.type.ResultCode;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.base.Preconditions;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

import java.util.List;

/**
 * build physical plan from logical plan of a regular query, the physical plan is actually denoted by ir core structure (FFI Pointer)
 */
public class FfiPhysicalBuilder extends RegularPhysicalBuilder<Pointer, byte[]> {
    private static final IrCoreLibrary LIB = IrCoreLibrary.INSTANCE;
    private final IrMeta irMeta;
    private final Configs graphConfig;
    private final Pointer ptrPlan;
    private int lastIdx;

    public FfiPhysicalBuilder(Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan) {
        super(
                logicalPlan,
                new GraphRelShuttleWrapper(new RelToFfiConverter(irMeta.getSchema().isColumnId())));
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
                appendProjectDedup(node, oprIdx);
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
        FfiData.ByValue ffiData =
                LIB.buildPhysicalPlan(ptrPlan, getEngineWorkerNum(), getEngineServerNum());
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

    private void appendMatch(PhysicalNode<Pointer> node, IntByReference oprIdx) {
        List<Pointer> ffiNodes = node.getNodes();
        Preconditions.checkArgument(
                ffiNodes.size() == 2,
                "should have 2 ffi nodes, one is `scan` and the other is `match`");
        checkFfiResult(LIB.appendScanOperator(ptrPlan, ffiNodes.get(0), oprIdx.getValue(), oprIdx));
        checkFfiResult(
                LIB.appendPatternOperator(ptrPlan, ffiNodes.get(1), oprIdx.getValue(), oprIdx));
    }

    private void appendProjectDedup(PhysicalNode<Pointer> node, IntByReference oprIdx) {
        List<Pointer> ffiNodes = node.getNodes();
        Preconditions.checkArgument(
                ffiNodes.size() == 2,
                "should have 2 ffi nodes, one is `project` and the other is `dedup`");
        checkFfiResult(
                LIB.appendProjectOperator(ptrPlan, ffiNodes.get(0), oprIdx.getValue(), oprIdx));
        checkFfiResult(
                LIB.appendDedupOperator(ptrPlan, ffiNodes.get(1), oprIdx.getValue(), oprIdx));
    }

    private void appendSink(IntByReference oprIdx) {
        Pointer ptrSink = LIB.initSinkOperator();
        checkFfiResult(LIB.appendSinkOperator(ptrPlan, ptrSink, oprIdx.getValue(), oprIdx));
    }

    private int getEngineWorkerNum() {
        switch (FrontendConfig.ENGINE_TYPE.get(this.graphConfig)) {
            case "pegasus":
                return PegasusConfig.PEGASUS_WORKER_NUM.get(graphConfig);
            case "hiactor":
            default:
                return 1;
        }
    }

    private int getEngineServerNum() {
        switch (FrontendConfig.ENGINE_TYPE.get(this.graphConfig)) {
            case "pegasus":
                return PegasusConfig.PEGASUS_HOSTS.get(graphConfig).split(",").length;
            case "hiactor":
            default:
                return 1;
        }
    }
}

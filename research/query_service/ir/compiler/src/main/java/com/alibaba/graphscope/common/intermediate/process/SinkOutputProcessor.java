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

package com.alibaba.graphscope.common.intermediate.process;

import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;
import com.alibaba.graphscope.common.exception.InterOpUnsupportedException;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import org.javatuples.Pair;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class SinkOutputProcessor implements InterOpProcessor {
    public static SinkOutputProcessor INSTANCE = new SinkOutputProcessor();

    private SinkOutputProcessor() {
    }

    @Override
    public void process(InterOpCollection opCollection) {
        SinkArg sinkArg = getSinkColumns(opCollection);
        if (sinkArg != null && !sinkArg.getColumnNames().isEmpty()) {
            SinkOp sinkOp = new SinkOp();
            sinkOp.setSinkArg(new OpArg(sinkArg, Function.identity()));
            opCollection.appendInterOp(sinkOp);
        }
    }

    private SinkArg getSinkColumns(InterOpCollection opCollection) {
        List<InterOpBase> collections = opCollection.unmodifiableCollection();
        SinkArg sinkArg = new SinkArg();
        for (int i = collections.size() - 1; i >= 0; --i) {
            InterOpBase cur = collections.get(i);
            if (cur instanceof DedupOp || cur instanceof LimitOp || cur instanceof OrderOp || cur instanceof SelectOp) {
                continue;
            } else if (cur instanceof ExpandOp || cur instanceof ScanFusionOp || cur instanceof GetVOp) {
                sinkArg.addColumnName(ArgUtils.asNoneTag());
                break;
            } else if (cur instanceof ProjectOp) {
                ProjectOp op = (ProjectOp) cur;
                List<Pair> exprWithAlias = (List<Pair>) op.getExprWithAlias().get().applyArg();
                for (Pair pair : exprWithAlias) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) pair.getValue1();
                    sinkArg.addColumnName(alias.alias);
                }
                break;
            } else if (cur instanceof GroupOp) {
                GroupOp op = (GroupOp) cur;
                List<Pair> groupKeys = (List<Pair>) op.getGroupByKeys().get().applyArg();
                for (Pair pair : groupKeys) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) pair.getValue1();
                    sinkArg.addColumnName(alias.alias);
                }
                List<ArgAggFn> groupValues = (List<ArgAggFn>) op.getGroupByValues().get().applyArg();
                for (ArgAggFn aggFn : groupValues) {
                    sinkArg.addColumnName(aggFn.getAlias().alias);
                }
                break;
            } else if (cur instanceof ApplyOp) {
                ApplyOp applyOp = (ApplyOp) cur;
                FfiJoinKind joinKind = (FfiJoinKind) applyOp.getJoinKind().get().applyArg();
                // where or not
                // order().by(), select().by(), group().by()
                if (joinKind == FfiJoinKind.Semi || joinKind == FfiJoinKind.Anti || joinKind == FfiJoinKind.Inner) {
                    continue;
                } else {
                    throw new InterOpUnsupportedException(cur.getClass(), "join kind is unsupported yet");
                }
            } else if (cur instanceof UnionOp) {
                UnionOp unionOp = (UnionOp) cur;
                Optional<OpArg> subOpsListOpt = unionOp.getSubOpCollectionList();
                if (!subOpsListOpt.isPresent()) {
                    throw new InterOpIllegalArgException(cur.getClass(), "subOpCollectionList", "is not present in union");
                }
                List<InterOpCollection> subOpsList = (List<InterOpCollection>) subOpsListOpt.get().applyArg();
                subOpsList.forEach(op -> {
                    SinkArg subSink = getSinkColumns(op);
                    subSink.getColumnNames().forEach(c -> sinkArg.addColumnName(c));
                });
                sinkArg.dedup();
                break;
            } else {
                throw new InterOpUnsupportedException(cur.getClass(), "unimplemented yet");
            }
        }
        return sinkArg;
    }
}
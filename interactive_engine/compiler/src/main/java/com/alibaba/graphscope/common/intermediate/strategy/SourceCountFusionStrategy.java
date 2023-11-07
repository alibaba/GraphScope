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

package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.google.common.collect.ImmutableList;

import org.javatuples.Pair;

import java.util.List;
import java.util.Optional;

public class SourceCountFusionStrategy implements InterOpStrategy {
    public static SourceCountFusionStrategy INSTANCE = new SourceCountFusionStrategy();

    private SourceCountFusionStrategy() {}

    @Override
    public void apply(InterOpCollection opCollection) {
        List<InterOpBase> original = opCollection.unmodifiableCollection();
        for (int i = original.size() - 2; i >= 0; --i) {
            InterOpBase cur = original.get(i);
            ArgAggFn next = nextCount(original, i);
            if (cur instanceof ScanFusionOp && !cur.getAlias().isPresent() && next != null) {
                ((ScanFusionOp) cur).setCountOnly(true);
                opCollection.replaceInterOp(i + 1, createSumOp(next));
            }
        }
    }

    // return ArgAggFn if next is count(), otherwise null
    private ArgAggFn nextCount(List<InterOpBase> original, int cur) {
        int next = cur + 1;
        if (next >= 0 && next < original.size() && original.get(next) instanceof GroupOp) {
            GroupOp groupOp = (GroupOp) original.get(next);
            Optional<OpArg> groupKeysOpt = groupOp.getGroupByKeys();
            Optional<OpArg> groupValuesOpt = groupOp.getGroupByValues();
            if (groupKeysOpt.isPresent()) {
                List<Pair> groupKeys = (List<Pair>) groupKeysOpt.get().applyArg();
                // groupKeys is empty means group by all
                if (groupKeys.isEmpty() && groupValuesOpt.isPresent()) {
                    List<ArgAggFn> groupValues = (List<ArgAggFn>) groupValuesOpt.get().applyArg();
                    if (groupValues.size() == 1
                            && groupValues.get(0).getAggregate() == FfiAggOpt.Count) {
                        return groupValues.get(0);
                    }
                }
            }
        }
        return null;
    }

    private GroupOp createSumOp(ArgAggFn count) {
        GroupOp sum = new GroupOp();
        sum.setGroupByKeys(new OpArg(ImmutableList.of()));
        sum.setGroupByValues(
                new OpArg(
                        ImmutableList.of(
                                new ArgAggFn(FfiAggOpt.Sum, count.getAlias(), count.getVar()))));
        return sum;
    }
}

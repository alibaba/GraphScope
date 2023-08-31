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

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;
import com.alibaba.graphscope.common.intermediate.operator.UnfoldOp;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.*;

import java.util.List;
import java.util.Optional;
import org.javatuples.Pair;

// fuse select as with the following unfold
public class UnfoldFusionStrategy implements InterOpStrategy {
    public static UnfoldFusionStrategy INSTANCE = new UnfoldFusionStrategy();

    private UnfoldFusionStrategy() {}

    @Override
    public void apply(InterOpCollection opCollection) {
        List<InterOpBase> original = opCollection.unmodifiableCollection();
        int off = 0;
        for (int i = 0; i < original.size(); ++i) {
            InterOpBase cur = original.get(i);
            UnfoldOp next = nextUnfold(original, i);
            if (cur instanceof ProjectOp && next != null) {
                ProjectOp projectOp = (ProjectOp) cur;
                List<Pair<String, FfiAlias.ByValue>> pairList =
                        (List<Pair<String, FfiAlias.ByValue>>)
                                projectOp.getExprWithAlias().get().getArg();
                Pair single = pairList.get(0);
                String tag_name = (String) single.getValue0();
                next.setUnfoldTag(new OpArg<>(ArgUtils.asAlias(tag_name.substring(1), true)));

                opCollection.removeInterOp(i - off);
                off += 1;
            }
        }
    }

    // return UnfoldOp if next is, otherwise null
    private UnfoldOp nextUnfold(List<InterOpBase> original, int cur) {
        int next = cur + 1;
        return (next >= 0 && next < original.size() && original.get(next) instanceof UnfoldOp)
                ? (UnfoldOp) original.get(next)
                : null;
    }
}

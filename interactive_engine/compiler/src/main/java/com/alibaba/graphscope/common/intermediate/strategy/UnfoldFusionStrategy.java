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

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.common.intermediate.operator.UnfoldOp;
import com.alibaba.graphscope.common.jna.type.*;

import org.javatuples.Pair;

import java.util.List;

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
                FfiAlias.ByValue alias_name;

                // skip for the following conditions (may be impossible, e.g., select("a",
                // "b").unfold())
                // select("a").as("b").unfold()
                // select("a", "b").as("c").unfold()
                // select("a", "b").as("c", "d").unfold()
                // [unsure]? select("a").by("name").unfold()
                if (projectOp.getAlias().isPresent()) {
                    alias_name = (FfiAlias.ByValue) projectOp.getAlias().get().getArg();
                    if (alias_name.alias.name != null) {
                        continue;
                    }
                }

                List<Pair<String, FfiAlias.ByValue>> pairList =
                        (List<Pair<String, FfiAlias.ByValue>>)
                                projectOp.getExprWithAlias().get().getArg();

                // select("a", "b").unfold()
                if (pairList.size() >= 2) {
                    continue;
                }

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

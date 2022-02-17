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

import java.util.List;
import java.util.Optional;

// fuse order with the following limit
public class TopKStrategy implements InterOpStrategy {
    public static TopKStrategy INSTANCE = new TopKStrategy();

    private TopKStrategy() {
    }

    @Override
    public void apply(InterOpCollection opCollection) {
        List<InterOpBase> original = opCollection.unmodifiableCollection();
        for (int i = original.size() - 2; i >= 0; --i) {
            InterOpBase cur = original.get(i);
            LimitOp next = nextLimit(original, i);
            if (cur instanceof OrderOp && next != null) {
                ((OrderOp) cur).setLower(next.getLower().get());
                ((OrderOp) cur).setUpper(next.getUpper().get());
                Optional<OpArg> nextAlias = next.getAlias();
                if (nextAlias.isPresent()) {
                    cur.setAlias(nextAlias.get());
                }
                opCollection.removeInterOp(i + 1);
            }
        }
    }

    // return LimitOp if next is, otherwise null
    private LimitOp nextLimit(List<InterOpBase> original, int cur) {
        int next = cur + 1;
        return (next >= 0 && next < original.size() &&
                original.get(next) instanceof LimitOp) ? (LimitOp) original.get(next) : null;
    }
}

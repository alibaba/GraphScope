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
import com.alibaba.graphscope.common.intermediate.operator.*;

import java.util.List;

// fuse ExpandOp + SelectOp, GetVOp + SelectOp
public class ElementFusionStrategy implements InterOpStrategy {
    public static ElementFusionStrategy INSTANCE = new ElementFusionStrategy();

    private ElementFusionStrategy() {}

    @Override
    public void apply(InterOpCollection opCollection) {
        List<InterOpBase> original = opCollection.unmodifiableCollection();
        for (int i = original.size() - 2; i >= 0; --i) {
            InterOpBase cur = original.get(i), next = original.get(i + 1);
            if (next instanceof SelectOp && (cur instanceof ExpandOp || cur instanceof GetVOp)) {
                QueryParams params =
                        (cur instanceof ExpandOp)
                                ? ((ExpandOp) cur).getParams().get()
                                : ((GetVOp) cur).getParams().get();
                String fuse = fusePredicates(params, (SelectOp) next);
                if (fuse != null && !fuse.isEmpty()) {
                    params.setPredicate(fuse);
                }
                opCollection.removeInterOp(i + 1);
            }
        }
    }

    private String fusePredicates(QueryParams params, SelectOp selectOp) {
        String p1 = params.getPredicate().isPresent() ? params.getPredicate().get() : null;
        String p2 =
                selectOp.getPredicate().isPresent()
                        ? (String) selectOp.getPredicate().get().applyArg()
                        : null;
        if (p2 == null || p2.isEmpty()) {
            return p1;
        } else if (p1 == null || p1.isEmpty()) {
            return p2;
        } else {
            return String.format("%s&&(%s)", p1, p2);
        }
    }
}

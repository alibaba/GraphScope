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
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;

import java.util.List;

// fuse outE + filter, outE + filter + has
public class ElementFusionStrategy implements InterOpStrategy {
    public static ElementFusionStrategy INSTANCE = new ElementFusionStrategy();

    private ElementFusionStrategy() {}

    @Override
    public void apply(InterOpCollection opCollection) {
        List<InterOpBase> original = opCollection.unmodifiableCollection();
        for (int i = original.size() - 2; i >= 0; --i) {
            InterOpBase cur = original.get(i), next = original.get(i + 1);
            // fuse outE + filter
            if (isExpandEdge(cur)
                    && next instanceof SelectOp
                    && ((SelectOp) next).getType() == SelectOp.FilterType.HAS) {
                QueryParams params = ((ExpandOp) cur).getParams().get();
                String fuse = fusePredicates(params, (SelectOp) next);
                if (fuse != null && !fuse.isEmpty()) {
                    params.setPredicate(fuse);
                }
                if (!cur.getAlias().isPresent() && next.getAlias().isPresent()) {
                    cur.setAlias(next.getAlias().get());
                }
                opCollection.removeInterOp(i + 1);
            }
            // fuse outE + inV
            if (isExpandEdge(cur)
                    && i + 1 < original.size()
                    && (next = original.get(i + 1)) instanceof GetVOp
                    && canFuseExpandWithGetV((ExpandOp) cur, (GetVOp) next)) {
                ((ExpandOp) cur).setEdgeOpt(new OpArg(false));
                if (next.getAlias().isPresent()) {
                    cur.setAlias(next.getAlias().get());
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

    private boolean isExpandEdge(InterOpBase op) {
        return op instanceof ExpandOp
                && (Boolean) ((ExpandOp) op).getIsEdge().get().applyArg() == true;
    }

    private boolean canFuseExpandWithGetV(ExpandOp expandOp, GetVOp getVOp) {
        FfiDirection direction = (FfiDirection) expandOp.getDirection().get().applyArg();
        FfiVOpt vOpt = (FfiVOpt) getVOp.getGetVOpt().get().applyArg();
        return (direction == FfiDirection.Out && vOpt == FfiVOpt.End // outE().inV()
                        || direction == FfiDirection.In && vOpt == FfiVOpt.Start // inE().outV()
                        || direction == FfiDirection.Both
                                && vOpt == FfiVOpt.Both) // bothE().bothV()
                && !isCurNeedAlias(expandOp, getVOp);
    }

    // outE().as("a").inV().as("b")
    // expand need a alias for latter attachment and cannot be fused with the next operator
    private boolean isCurNeedAlias(InterOpBase cur, InterOpBase next) {
        return cur.getAlias().isPresent()
                && (!next.getAlias().isPresent()
                        || !cur.getAlias()
                                .get()
                                .applyArg()
                                .equals(next.getAlias().get().applyArg()));
    }
}

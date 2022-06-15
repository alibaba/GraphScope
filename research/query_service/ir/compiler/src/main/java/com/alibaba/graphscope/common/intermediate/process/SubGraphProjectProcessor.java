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

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;

import java.util.List;

// the getEOp and getVOp need all properties to be stored in subgraph,
// process all operations and set all columns in QueryParams
public class SubGraphProjectProcessor implements InterOpProcessor {
    public static SubGraphProjectProcessor INSTANCE = new SubGraphProjectProcessor();

    private SubGraphProjectProcessor() {
    }

    @Override
    public void process(InterOpCollection opCollection) {
        List<InterOpBase> ops = opCollection.unmodifiableCollection();
        int size = ops.size();
        for (int i = size - 1; i >= 0; --i) {
            InterOpBase op = ops.get(i);
            if (op instanceof SubGraphAsUnionOp) {
                // need all properties of the edges, set params as all
                for (int j = i - 1; j >= 0; --j) {
                    InterOpBase getEOp = ops.get(j);
                    if (getEOp instanceof ExpandOp || getEOp instanceof ScanFusionOp) {
                        QueryParams params = (getEOp instanceof ExpandOp) ?
                                ((ExpandOp) getEOp).getParams().get() : ((ScanFusionOp) getEOp).getParams().get();
                        params.setAllColumns(true);
                        break;
                    }
                }
                // need all properties of the bothV of the edges, set params as all
                List<InterOpCollection> subGraphOps =
                        (List<InterOpCollection>) ((SubGraphAsUnionOp) op).getSubOpCollectionList().get().applyArg();
                if (subGraphOps.size() > 1) {
                    InterOpCollection getVOps = subGraphOps.get(1);
                    for (InterOpBase opBase : getVOps.unmodifiableCollection()) {
                        if (opBase instanceof GetVOp) {
                            QueryParams params = ((GetVOp) opBase).getParams().get();
                            params.setAllColumns(true);
                            break;
                        }
                    }
                }
                return;
            }
        }
    }
}

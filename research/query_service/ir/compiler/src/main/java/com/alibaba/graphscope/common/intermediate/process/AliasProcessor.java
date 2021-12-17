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
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;

import java.util.ArrayList;
import java.util.List;

// process alias of each InterOp, some aliases need to be set by Auxilia
public class AliasProcessor implements InterOpProcessor {
    public static AliasProcessor INSTANCE = new AliasProcessor();

    @Override
    public void process(InterOpCollection opCollection) {
        List<InterOpBase> copy = new ArrayList<>(opCollection.unmodifiableCollection());
        for (int i = copy.size() - 1; i >= 0; --i) {
            InterOpBase op = copy.get(i);
            if (op instanceof ScanFusionOp || op instanceof ExpandOp || op instanceof AuxiliaOp) {
                continue;
            }
            if (op instanceof ProjectOp && op.getAlias().isPresent()) {
                throw new InterOpIllegalArgException(op.getClass(), "project alias", "unsupported yet");
            }
            if (op.getAlias().isPresent()) {
                List<InterOpBase> original = opCollection.unmodifiableCollection();
                InterOpBase next = (i + 1 < original.size() && original.get(i + 1) instanceof AuxiliaOp) ? original.get(i + 1) : null;
                if (next != null) {
                    next.setAlias(op.getAlias().get());
                } else {
                    // add auxilia to reserve tags of the previous op
                    AuxiliaOp auxiliaOp = new AuxiliaOp();
                    auxiliaOp.setAlias(op.getAlias().get());
                    opCollection.insertInterOp(i + 1, auxiliaOp);
                }
                op.clearAlias();
            }
        }
    }
}

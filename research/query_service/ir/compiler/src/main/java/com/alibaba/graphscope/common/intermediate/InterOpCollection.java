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

package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;
import com.alibaba.graphscope.common.intermediate.operator.ApplyOp;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.process.InterOpProcessor;
import com.alibaba.graphscope.common.intermediate.process.SinkOutputProcessor;
import com.alibaba.graphscope.common.intermediate.strategy.ElementFusionStrategy;
import com.alibaba.graphscope.common.intermediate.strategy.InterOpStrategy;
import com.alibaba.graphscope.common.intermediate.strategy.TopKStrategy;

import org.apache.commons.collections.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

// collection of intermediate operators
public class InterOpCollection {
    private List<InterOpBase> opCollection;
    private static List<InterOpStrategy> strategies =
            Arrays.asList(TopKStrategy.INSTANCE, ElementFusionStrategy.INSTANCE);
    private static List<InterOpProcessor> processors = Arrays.asList(SinkOutputProcessor.INSTANCE);

    public InterOpCollection() {
        opCollection = new ArrayList<>();
    }

    public List<InterOpBase> unmodifiableCollection() {
        return UnmodifiableList.decorate(this.opCollection);
    }

    public void appendInterOp(InterOpBase op) {
        this.opCollection.add(op);
    }

    public void removeInterOp(int i) {
        opCollection.remove(i);
    }

    public static void applyStrategies(InterOpCollection opCollection) {
        opCollection
                .unmodifiableCollection()
                .forEach(
                        op -> {
                            if (op instanceof ApplyOp) {
                                ApplyOp applyOp = (ApplyOp) op;
                                Optional<OpArg> subOps = applyOp.getSubOpCollection();
                                if (!subOps.isPresent()) {
                                    throw new InterOpIllegalArgException(
                                            op.getClass(),
                                            "subOpCollection",
                                            "is not present in apply");
                                }
                                InterOpCollection subCollection =
                                        (InterOpCollection) subOps.get().applyArg();
                                applyStrategies(subCollection);
                            }
                        });
        strategies.forEach(k -> k.apply(opCollection));
    }

    public static void process(InterOpCollection opCollection) {
        // only traverse root opCollection for SinkOutputProcessor
        processors.forEach(k -> k.process(opCollection));
    }
}

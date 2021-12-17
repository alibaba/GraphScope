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

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.process.AliasProcessor;
import com.alibaba.graphscope.common.intermediate.process.InterOpProcessor;
import com.alibaba.graphscope.common.intermediate.process.PropertyDetailsProcessor;
import org.apache.commons.collections.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// collection of intermediate operators
public class InterOpCollection {
    private List<InterOpBase> opCollection;
    private static List<InterOpProcessor> processors = Arrays.asList(PropertyDetailsProcessor.INSTANCE, AliasProcessor.INSTANCE);

    public InterOpCollection() {
        opCollection = new ArrayList<>();

    }

    public IrPlan buildIrPlan() {
        process();
        IrPlan irPlan = new IrPlan();
        unmodifiableCollection().forEach(k -> {
            irPlan.appendInterOp(k);
        });
        return irPlan;
    }

    public List<InterOpBase> unmodifiableCollection() {
        return UnmodifiableList.decorate(this.opCollection);
    }

    public void appendInterOp(InterOpBase op) {
        this.opCollection.add(op);
    }

    public void insertInterOp(int i, InterOpBase op) {
        opCollection.add(i, op);
    }

    private void process() {
        processors.forEach(k -> k.process(this));
    }
}

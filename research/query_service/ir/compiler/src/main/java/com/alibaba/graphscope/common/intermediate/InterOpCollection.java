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
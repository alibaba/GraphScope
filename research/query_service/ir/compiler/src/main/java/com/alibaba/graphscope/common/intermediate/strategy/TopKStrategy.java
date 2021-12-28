package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;

import java.util.List;

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

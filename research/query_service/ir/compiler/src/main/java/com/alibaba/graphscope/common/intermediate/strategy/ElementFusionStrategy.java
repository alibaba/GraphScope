package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;

import java.util.List;

// fuse out + has, inV + has
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

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

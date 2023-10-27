package com.alibaba.graphscope.common.ir.rex;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.mapping.Mappings;

public class RexPermuteGraphShuttle extends RexPermuteInputsShuttle {
    private final Mappings.TargetMapping mapping;

    /**
     * Creates a RexPermuteInputsShuttle.
     *
     * <p>The mapping provides at most one target for every source. If a source
     * has no targets and is referenced in the expression,
     * {@link Mappings.TargetMapping#getTarget(int)}
     * will give an error. Otherwise the mapping gives a unique target.
     *
     * @param mapping Mapping
     * @param inputs  Input relational expressions
     */
    public RexPermuteGraphShuttle(Mappings.TargetMapping mapping, RelNode... inputs) {
        super(mapping, inputs);
        this.mapping = mapping;
    }

    @Override
    public RexNode visitInputRef(RexInputRef local) {
        final int index = local.getIndex();
        int target = mapping.getTarget(index);
        return (local instanceof RexGraphVariable)
                ? visitGraphVariable((RexGraphVariable) local)
                : new RexInputRef(target, local.getType());
    }

    public RexNode visitGraphVariable(RexGraphVariable variable) {
        final int index = variable.getIndex(); // resource column id
        int target = mapping.getTarget(index);

        return variable.getProperty() == null
                ? RexGraphVariable.of(
                        variable.getAliasId(), target, variable.getName(), variable.getType())
                : RexGraphVariable.of(
                        variable.getAliasId(),
                        variable.getProperty(),
                        target,
                        variable.getName(),
                        variable.getType());
    }
}

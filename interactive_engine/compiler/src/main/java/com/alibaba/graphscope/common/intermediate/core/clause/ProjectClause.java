package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.IrNode;
import com.alibaba.graphscope.common.intermediate.core.IrNodeList;
import com.alibaba.graphscope.common.intermediate.core.IrOperatorKind;

import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * maintain a list of pairs, each pair consists of expression and alias,
 * and can also be denoted as a {@code IrNode} by using {@link IrOperatorKind#AS}.
 */
public class ProjectClause extends AbstractClause {
    private IrNodeList projectList;

    public ProjectClause addProject(@Nullable String alias, IrNode expr) {
        throw new NotImplementedException();
    }
}

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class ProjectOp extends InterOpBase {
    // list of Pair<expr, AliasArg>
    private Optional<OpArg> projectExprWithAlias;

    public ProjectOp() {
        projectExprWithAlias = Optional.empty();
    }

    public Optional<OpArg> getProjectExprWithAlias() {
        return projectExprWithAlias;
    }

    public void setProjectExprWithAlias(OpArg projectExprWithAlias) {
        this.projectExprWithAlias = Optional.of(projectExprWithAlias);
    }
}

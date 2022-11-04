package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.IrNodeList;

import org.apache.commons.lang.NotImplementedException;

public class LimitClause extends AbstractClause {
    // represent range as a two-elements list of IrLiterals
    private IrNodeList range;

    public LimitClause setLower(int lower) {
        throw new NotImplementedException();
    }

    public LimitClause setUpper(int upper) {
        throw new NotImplementedException();
    }
}

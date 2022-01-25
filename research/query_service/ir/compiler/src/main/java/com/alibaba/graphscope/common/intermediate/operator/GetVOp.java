package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class GetVOp extends InterOpBase {
    // FfiVOpt
    private Optional<OpArg> getVOpt;

    public GetVOp() {
        super();
        getVOpt = Optional.empty();
    }

    public Optional<OpArg> getGetVOpt() {
        return getVOpt;
    }

    public void setGetVOpt(OpArg getVOpt) {
        this.getVOpt = Optional.of(getVOpt);
    }
}

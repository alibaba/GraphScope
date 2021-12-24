package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class DedupOp extends InterOpBase {
    // list<FfiVariable>
    private Optional<OpArg> dedupKeys;

    public DedupOp() {
        super();
        dedupKeys = Optional.empty();
    }

    public Optional<OpArg> getDedupKeys() {
        return dedupKeys;
    }

    public void setDedupKeys(OpArg dedupKeys) {
        this.dedupKeys = Optional.of(dedupKeys);
    }
}
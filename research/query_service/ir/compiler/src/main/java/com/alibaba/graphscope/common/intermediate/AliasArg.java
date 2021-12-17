package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.google.common.base.Objects;

public class AliasArg {
    private FfiNameOrId.ByValue alias;
    private boolean isQueryGiven;

    public AliasArg(FfiNameOrId.ByValue alias) {
        this.alias = alias;
        // set true by default
        this.isQueryGiven = true;
    }

    public AliasArg(FfiNameOrId.ByValue alias, boolean isQueryGiven) {
        this.alias = alias;
        this.isQueryGiven = isQueryGiven;
    }

    public FfiNameOrId.ByValue getAlias() {
        return alias;
    }

    public boolean isQueryGiven() {
        return isQueryGiven;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasArg aliasArg = (AliasArg) o;
        return isQueryGiven == aliasArg.isQueryGiven &&
                Objects.equal(alias, aliasArg.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(alias, isQueryGiven);
    }
}

package com.alibaba.graphscope.common.jna.type;

import com.google.common.base.Objects;
import com.sun.jna.Structure;

@Structure.FieldOrder({"alias", "isQueryGiven"})
public class FfiAlias extends Structure {
    public static class ByValue extends FfiAlias implements Structure.ByValue {
    }

    public FfiNameOrId.ByValue alias;
    public boolean isQueryGiven;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FfiAlias ffiAlias = (FfiAlias) o;
        return isQueryGiven == ffiAlias.isQueryGiven &&
                Objects.equal(alias, ffiAlias.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(alias, isQueryGiven);
    }
}
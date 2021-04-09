package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.google.common.base.MoreObjects;

/**
 * The edge value type
 */
public class EdgeValueType implements ValueType {

    @Override
    public int hashCode() {
        return this.getClass().getName().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (null == that || getClass() != that.getClass()) {
            return false;
        }

        return true;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
}

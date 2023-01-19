package com.alibaba.graphscope.common.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * denote variables in ir, i.e. "a" or "name" or "a.name"
 */
public class RexGraphVariable extends RexNode {
    /**
     * create variable from a single alias, i.e. "a"
     * @param aliasId todo: use a MAGIC_NUM to denote `head` in gremlin
     * @param type
     * @return
     */
    public static RexGraphVariable of(int aliasId, RelDataType type) {
        return null;
    }

    /**
     * create variable from a pair of alias and fieldName, i.e. "a.name"
     * @param aliasId todo: use a MAGIC_NUM to denote `head` in gremlin
     * @param fieldId
     * @param type
     * @return
     */
    public static RexGraphVariable of(int aliasId, int fieldId, RelDataType type) {
        return null;
    }

    @Override
    public RelDataType getType() {
        return null;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public <R> R accept(RexVisitor<R> rexVisitor) {
        throw new NotImplementedException("");
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> rexBiVisitor, P p) {
        throw new NotImplementedException("");
    }
}

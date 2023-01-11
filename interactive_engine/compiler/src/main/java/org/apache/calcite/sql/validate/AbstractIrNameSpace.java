package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractIrNameSpace extends AbstractNamespace {
    public AbstractIrNameSpace() {
        super(null, null);
    }

    @Override
    public @Nullable SqlNode getNode() {
        return null;
    }
}

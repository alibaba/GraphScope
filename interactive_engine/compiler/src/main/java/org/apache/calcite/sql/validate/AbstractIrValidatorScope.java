package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;

public abstract class AbstractIrValidatorScope extends DelegatingScope {
    public AbstractIrValidatorScope() {
        super(null);
    }

    @Override
    public SqlNode getNode() {
        return null;
    }
}

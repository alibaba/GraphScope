package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary key constraint for vertex type
 */
public class PrimaryKeyConstraint {
    private List<String> primaryKeyList;

    public PrimaryKeyConstraint(List<String> primaryKeyList) {
        if (primaryKeyList == null || primaryKeyList.isEmpty()) {
            throw new GraphCreateSchemaException("primary key cant be null or empty");
        }
        this.primaryKeyList = primaryKeyList;
    }

    public List<String> getPrimaryKeyList() {
        return ImmutableList.copyOf(this.primaryKeyList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("primaryKeyList", primaryKeyList)
                .toString();
    }
}

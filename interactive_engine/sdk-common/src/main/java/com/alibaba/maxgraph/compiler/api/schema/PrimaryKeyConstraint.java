/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.api.schema;

import com.alibaba.maxgraph.compiler.api.exception.GraphSchemaException;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** Primary key constraint for vertex type */
public class PrimaryKeyConstraint {
    private List<String> primaryKeyList;

    public PrimaryKeyConstraint(List<String> primaryKeyList) {
        if (primaryKeyList == null || primaryKeyList.isEmpty()) {
            throw new GraphSchemaException("primary key cant be null or empty");
        }
        this.primaryKeyList = primaryKeyList;
    }

    public List<String> getPrimaryKeyList() {
        return ImmutableList.copyOf(this.primaryKeyList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("primaryKeyList", primaryKeyList).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryKeyConstraint that = (PrimaryKeyConstraint) o;
        return Objects.equals(primaryKeyList, that.primaryKeyList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKeyList);
    }
}

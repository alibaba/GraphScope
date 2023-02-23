/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Denote variables, i.e. "a" or "name" or "a.name"
 */
public class RexGraphVariable extends RexInputRef {
    // null -> 'head' in gremlin
    private @Nullable Integer aliasId;
    // null -> get object referred by the given alias, i.e. 'a'
    // not null -> get object referred by the given alias and get the given property from it, i.e.
    // 'a.name'
    private @Nullable Integer propertyId;

    protected RexGraphVariable(int aliasId, @Nullable String name, RelDataType type) {
        this(name, type);
        this.aliasId = aliasId;
    }

    protected RexGraphVariable(
            int aliasId, int propertyId, @Nullable String name, RelDataType type) {
        this(name, type);
        this.aliasId = aliasId;
        this.propertyId = propertyId;
    }

    protected RexGraphVariable(@Nullable String name, RelDataType type) {
        super(0, type);
        this.digest = (name == null) ? StringUtils.EMPTY : name;
    }

    /**
     * create variable from a single alias, i.e. "a"
     * @param aliasId todo: use a MAGIC_NUM to denote `head` in gremlin
     * @param name unique name to identify the variable, i.e. HEAD or a
     * @param type
     * @return
     */
    public static RexGraphVariable of(int aliasId, @Nullable String name, RelDataType type) {
        return new RexGraphVariable(aliasId, name, type);
    }

    /**
     * create variable from a pair of alias and fieldName, i.e. "a.name"
     * @param aliasId todo: use a MAGIC_NUM to denote `head` in gremlin
     * @param fieldId
     * @param name unique name to identify the variable, i.e. HEAD.name or a.age
     * @param type
     * @return
     */
    public static RexGraphVariable of(
            int aliasId, int fieldId, @Nullable String name, RelDataType type) {
        return new RexGraphVariable(aliasId, fieldId, name, type);
    }

    public @Nullable Integer getAliasId() {
        return aliasId;
    }

    public @Nullable Integer getPropertyId() {
        return propertyId;
    }

    @Override
    public <R> R accept(RexVisitor<R> rexVisitor) {
        if (rexVisitor instanceof RexVariableAliasChecker
                || rexVisitor instanceof RexVariableConverter) {
            return rexVisitor.visitInputRef(this);
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RexGraphVariable that = (RexGraphVariable) o;
        return Objects.equals(aliasId, that.aliasId) && Objects.equals(propertyId, that.propertyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aliasId, propertyId);
    }

    @Override
    public String getName() {
        return this.digest;
    }
}

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

import com.alibaba.graphscope.common.ir.runtime.RexToLogicPBConverter;
import com.alibaba.graphscope.common.ir.type.GraphProperty;

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
    private int aliasId;

    // null -> get object referred by the given alias, i.e. 'a'
    // not null -> get object referred by the given alias and get the given property from it, i.e.
    // 'a.name'
    private @Nullable GraphProperty property;

    protected RexGraphVariable(int aliasId, @Nullable String name, RelDataType type) {
        this(name, type);
        this.aliasId = aliasId;
    }

    protected RexGraphVariable(
            int aliasId, GraphProperty property, @Nullable String name, RelDataType type) {
        this(name, type);
        this.aliasId = aliasId;
        this.property = Objects.requireNonNull(property);
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
     * @param property
     * @param name unique name to identify the variable, i.e. HEAD.name or a.age
     * @param type
     * @return
     */
    public static RexGraphVariable of(
            int aliasId, GraphProperty property, @Nullable String name, RelDataType type) {
        return new RexGraphVariable(aliasId, property, name, type);
    }

    @Override
    public <R> R accept(RexVisitor<R> rexVisitor) {
        if (rexVisitor instanceof RexVariableAliasChecker
                || rexVisitor instanceof RexVariableConverter
                || rexVisitor instanceof RexToLogicPBConverter) {
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
        return aliasId == that.aliasId && Objects.equals(property, that.property);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aliasId, property);
    }

    @Override
    public String getName() {
        return this.digest;
    }

    public @Nullable int getAliasId() {
        return aliasId;
    }

    public @Nullable GraphProperty getProperty() {
        return property;
    }
}

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

package com.alibaba.graphscope.common.calcite.rex;

import static com.alibaba.graphscope.common.calcite.util.Static.RESOURCE;

import com.alibaba.graphscope.common.calcite.util.Static;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Denote variables, i.e. "a" or "name" or "a.name"
 */
public class RexGraphVariable extends RexInputRef {
    private List<Integer> idList;

    protected RexGraphVariable(int aliasId, @Nullable String name, RelDataType type) {
        this(name, type);
        this.idList = ImmutableList.of(aliasId);
    }

    protected RexGraphVariable(int aliasId, int fieldId, @Nullable String name, RelDataType type) {
        this(name, type);
        this.idList = ImmutableList.of(aliasId, fieldId);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RexGraphVariable that = (RexGraphVariable) o;
        return Objects.equal(idList, that.idList) && Objects.equal(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(idList, type);
    }

    @Override
    public <R> R accept(RexVisitor<R> rexVisitor) {
        if (rexVisitor instanceof RexVariableAliasChecker) {
            return rexVisitor.visitInputRef(this);
        } else {
            return null;
        }
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> rexBiVisitor, P p) {
        throw RESOURCE.functionWillImplement(this.getClass()).ex();
    }

    int getAliasId() {
        return idList.isEmpty() ? Static.Alias.DEFAULT_ID : idList.get(0);
    }

    int getPropertyId() {
        return idList.size() < 2 ? -1 : idList.get(1);
    }

    @Override
    public String getName() {
        return this.digest;
    }
}

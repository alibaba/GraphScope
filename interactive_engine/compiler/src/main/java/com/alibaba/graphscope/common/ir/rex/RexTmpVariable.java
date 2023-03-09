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

import com.alibaba.graphscope.common.ir.tools.AliasInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * maintain the necessary info to create a {@code RexGraphVariable}
 */
public class RexTmpVariable extends RexInputRef {
    private final @Nullable String alias;
    private final @Nullable String property;
    private final RelDataType type;

    protected RexTmpVariable(@Nullable String alias, RelDataType type) {
        this(alias, null, type);
    }

    protected RexTmpVariable(@Nullable String alias, @Nullable String property, RelDataType type) {
        super(0, type);
        this.digest =
                (alias == null)
                        ? StringUtils.EMPTY
                        : alias
                                + ((property == null)
                                        ? StringUtils.EMPTY
                                        : AliasInference.DELIMITER + property);
        this.alias = alias;
        this.property = property;
        this.type = type;
    }

    public static RexTmpVariable of(@Nullable String alias, RelDataType type) {
        return new RexTmpVariable(alias, type);
    }

    public static RexTmpVariable of(@Nullable String alias, String property, RelDataType type) {
        return new RexTmpVariable(alias, property, type);
    }

    @Override
    public RelDataType getType() {
        return this.type;
    }

    @Override
    public <R> R accept(RexVisitor<R> rexVisitor) {
        return (rexVisitor instanceof RexVariableConverter) ? rexVisitor.visitInputRef(this) : null;
    }

    @Override
    public String getName() {
        return this.digest;
    }

    public @Nullable String getAlias() {
        return alias;
    }

    public @Nullable String getProperty() {
        return property;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RexTmpVariable that = (RexTmpVariable) o;
        return Objects.equals(alias, that.alias)
                && Objects.equals(property, that.property)
                && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), alias, property, type);
    }
}

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

package com.alibaba.graphscope.common.ir.rel.type.group;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * maintains group keys and aliases
 */
public class GraphGroupKeys implements RelBuilder.GroupKey {
    private final List<RexNode> variables;
    private final List<@Nullable String> aliases;

    public GraphGroupKeys(List<RexNode> variables, List<@Nullable String> aliases) {
        this.variables = Objects.requireNonNull(variables);
        this.aliases = Objects.requireNonNull(aliases);
    }

    @Override
    public RelBuilder.GroupKey alias(@Nullable String s) {
        return this;
    }

    @Override
    public int groupKeyCount() {
        return this.variables.size();
    }

    public List<RexNode> getVariables() {
        return Collections.unmodifiableList(variables);
    }

    public List<@Nullable String> getAliases() {
        return Collections.unmodifiableList(aliases);
    }

    @Override
    public String toString() {
        return "{" + "variables=" + variables + ", aliases=" + aliases + '}';
    }
}

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

/**
 * implement interfaces of {@code AggCall} by default, these interfaces are useless to {@code GraphAggCall}
 */
public abstract class AbstractAggCall implements RelBuilder.AggCall {
    @Override
    public RelBuilder.AggCall as(@Nullable String alias) {
        return this;
    }

    @Override
    public RelBuilder.AggCall distinct(boolean distinct) {
        return this;
    }

    @Override
    public RelBuilder.AggCall filter(@Nullable RexNode rexNode) {
        return this;
    }

    @Override
    public RelBuilder.AggCall sort(Iterable<RexNode> iterable) {
        return this;
    }

    @Override
    public RelBuilder.AggCall unique(@Nullable Iterable<RexNode> iterable) {
        return this;
    }

    @Override
    public RelBuilder.AggCall approximate(boolean b) {
        return this;
    }

    @Override
    public RelBuilder.AggCall ignoreNulls(boolean b) {
        return this;
    }

    @Override
    public RelBuilder.OverCall over() {
        throw new UnsupportedOperationException("over is unsupported for we will never use it");
    }
}

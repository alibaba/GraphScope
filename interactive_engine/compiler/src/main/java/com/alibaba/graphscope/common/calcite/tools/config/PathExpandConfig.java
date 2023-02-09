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

package com.alibaba.graphscope.common.calcite.tools.config;

import com.alibaba.graphscope.common.jna.type.PathOpt;
import com.alibaba.graphscope.common.jna.type.ResultOpt;

import org.checkerframework.checker.nullness.qual.Nullable;

public class PathExpandConfig {
    private final ExpandConfig expandConfig;
    private final GetVConfig getVConfig;
    @Nullable private final String alias;
    private int offset;
    private int fetch;
    private PathOpt pathOpt;
    private ResultOpt resultOpt;

    public PathExpandConfig(ExpandConfig config1, GetVConfig config2) {
        this(config1, config2, null);
    }

    public PathExpandConfig(ExpandConfig config1, GetVConfig config2, @Nullable String alias) {
        this.expandConfig = config1;
        this.getVConfig = config2;
        this.alias = alias;
        this.pathOpt = PathOpt.Arbitrary;
        this.resultOpt = ResultOpt.EndV;
    }

    public PathExpandConfig pathOpt(PathOpt pathOpt) {
        this.pathOpt = pathOpt;
        return this;
    }

    public PathExpandConfig resultOpt(ResultOpt resultOpt) {
        this.resultOpt = resultOpt;
        return this;
    }

    public PathExpandConfig range(int offset, int fetch) {
        this.offset = offset;
        this.fetch = fetch;
        return this;
    }

    public ExpandConfig getExpandConfig() {
        return expandConfig;
    }

    public GetVConfig getGetVConfig() {
        return getVConfig;
    }

    public String getAlias() {
        return alias;
    }

    public PathOpt getPathOpt() {
        return pathOpt;
    }

    public ResultOpt getResultOpt() {
        return resultOpt;
    }

    public int getOffset() {
        return offset;
    }

    public int getFetch() {
        return fetch;
    }
}

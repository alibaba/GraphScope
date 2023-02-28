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

package com.alibaba.graphscope.common.ir.type;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class GraphProperty {
    public static final String LEN_KEY = "~len";
    public static final String ALL_KEY = "~all";
    public static final String ID_KEY = "~id";
    public static final String LABEL_KEY = "~label";

    private final Opt opt;
    private final @Nullable GraphNameOrId key;

    public GraphProperty(Opt opt) {
        this.opt = opt;
        this.key = null;
    }

    public GraphProperty(GraphNameOrId key) {
        this.key = Objects.requireNonNull(key);
        this.opt = Opt.KEY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphProperty that = (GraphProperty) o;
        return opt == that.opt && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opt, key);
    }

    public Opt getOpt() {
        return opt;
    }

    public @Nullable GraphNameOrId getKey() {
        return key;
    }

    public enum Opt {
        ID,
        LABEL,
        LEN,
        ALL,
        KEY
    }

}

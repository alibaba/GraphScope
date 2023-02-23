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

public class NameOrId {
    private @Nullable String name;
    private int id;
    private final Opt opt;

    public NameOrId(String name) {
        this.name = Objects.requireNonNull(name);
        this.opt = Opt.NAME;
    }

    public NameOrId(int id) {
        this.id = id;
        this.opt = Opt.ID;
    }

    public @Nullable String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public Opt getOpt() {
        return opt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NameOrId nameOrId = (NameOrId) o;
        return id == nameOrId.id && Objects.equals(name, nameOrId.name) && opt == nameOrId.opt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, opt);
    }

    public enum Opt {
        NAME,
        ID
    }
}

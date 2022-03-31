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

package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.google.common.base.Objects;
import com.sun.jna.Structure;

@Structure.FieldOrder({"opt", "key"})
public class FfiProperty extends Structure {
    public FfiProperty() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiProperty implements Structure.ByValue {}

    public FfiPropertyOpt opt;
    public FfiNameOrId.ByValue key;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FfiProperty that = (FfiProperty) o;
        return opt == that.opt && Objects.equal(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(opt, key);
    }
}

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
import com.sun.jna.Structure;

@Structure.FieldOrder({"opt", "name", "nameId"})
public class FfiNameOrId extends Structure {
    private FfiNameOrId() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiNameOrId implements Structure.ByValue {
    }

    public FfiNameIdOpt opt;
    public String name;
    public int nameId;

    public static FfiNameOrId create() {
        return new FfiNameOrId();
    }

    public FfiNameOrId name(String name) {
        this.opt = FfiNameIdOpt.Name;
        this.name = name;
        return this;
    }

    public FfiNameOrId nameId(int nameId) {
        this.opt = FfiNameIdOpt.Id;
        this.nameId = nameId;
        return this;
    }
}

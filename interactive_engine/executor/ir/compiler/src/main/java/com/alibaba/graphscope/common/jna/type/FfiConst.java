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
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

@Structure.FieldOrder({"dataType", "bool", "int32", "int64", "float64", "cstr", "raw"})
public class FfiConst extends Structure {
    public FfiConst() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiConst implements Structure.ByValue {}

    public FfiDataType dataType;
    public boolean bool;
    public int int32;
    public long int64;
    public double float64;
    public String cstr;
    public Pointer raw;
}

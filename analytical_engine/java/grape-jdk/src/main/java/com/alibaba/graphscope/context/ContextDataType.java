/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.context;

import com.alibaba.fastffi.CXXEnum;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeRefiner;
import com.alibaba.graphscope.utils.CppClassName;

@FFITypeAlias(CppClassName.CONTEXT_DATA_TYPE)
@FFITypeRefiner("com.alibaba.graphscope.context.ContextDataType.get")
public enum ContextDataType implements CXXEnum {
    kBool,
    kInt32,
    kInt64,
    kUInt32,
    kUInt64,
    kFloat,
    kDouble,
    kString,
    kUndefined;

    public static ContextDataType get(int value) {
        switch (value) {
            case 0:
                return kBool;
            case 1:
                return kInt32;
            case 2:
                return kInt64;
            case 3:
                return kUInt32;
            case 4:
                return kUInt64;
            case 5:
                return kFloat;
            case 6:
                return kDouble;
            case 7:
                return kString;
            case 8:
                return kUndefined;
            default:
                throw new IllegalStateException("Unknow value for Context data type: " + value);
        }
    }

    @Override
    public int getValue() {
        return ordinal();
    }
}

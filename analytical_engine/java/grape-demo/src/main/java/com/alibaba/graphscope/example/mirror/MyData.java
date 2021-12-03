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

package com.alibaba.graphscope.example.mirror;

import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIJava;
import com.alibaba.fastffi.FFIMirror;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFISetter;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("MyData")
public interface MyData extends CXXPointer, FFIJava {
    Factory factory = FFITypeFactory.getFactory(MyData.class);

    static MyData create() {
        return factory.create();
    }

    @FFIGetter
    long id();

    @FFISetter
    void id(long value);

    default int javaHashCode() {
        return (int) id();
    }

    default String toJavaString() {
        return String.valueOf(id());
    }

    default boolean javaEquals(Object obj) {
        if (obj instanceof MyData) {
            MyData other = (MyData) obj;
            return id() == other.id();
        } else {
            return false;
        }
    }

    @FFIFactory
    interface Factory {
        MyData create();
    }
}

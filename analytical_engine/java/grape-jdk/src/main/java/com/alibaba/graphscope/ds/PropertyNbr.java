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

package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointerRangeElement;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.PROPERTY_NBR)
public interface PropertyNbr<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbr<VID_T>> {
    @FFINameAlias("neighbor")
    @CXXValue
    Vertex<VID_T> neighbor();

    @FFINameAlias("get_double")
    double getDouble(int propertyId);

    @FFINameAlias("get_int")
    int getInt(int propertyId);

    @FFINameAlias("get_str")
    @CXXValue
    FFIByteString getString(int propertyId);

    @CXXOperator("++")
    @CXXReference
    PropertyNbr<VID_T> inc();

    @CXXOperator("==")
    boolean eq(@CXXReference PropertyNbr<VID_T> rhs);

    @CXXOperator("--")
    @CXXReference
    PropertyNbr<VID_T> dec();

    @FFIFactory
    interface Factory<VID_T> {
        PropertyNbr<VID_T> create();
    }
}

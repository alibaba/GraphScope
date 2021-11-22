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
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;
import java.util.Iterator;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.PROPERTY_ADJ_LIST)
public interface PropertyAdjList<VID_T> extends FFIPointer, CXXPointer {
    @FFINameAlias("begin")
    @CXXValue
    PropertyNbr<VID_T> begin();

    @FFINameAlias("end")
    @CXXValue
    PropertyNbr<VID_T> end();

    @FFINameAlias("begin_unit")
    PropertyNbrUnit<VID_T> beginUnit();

    @FFINameAlias("end_unit")
    PropertyNbrUnit<VID_T> endUnit();

    @FFINameAlias("Size")
    int size();

    @FFINameAlias("Empty")
    boolean empty();

    default Iterable<PropertyNbr<VID_T>> iterator() {
        return () ->
                new Iterator<PropertyNbr<VID_T>>() {
                    PropertyNbr<VID_T> cur = begin().dec();
                    PropertyNbr<VID_T> end = end();
                    boolean flag = false;

                    @Override
                    public boolean hasNext() {
                        if (!flag) {
                            cur = cur.inc();
                            flag = !cur.eq(end);
                        }
                        return flag;
                    }

                    @Override
                    public PropertyNbr<VID_T> next() {
                        flag = false;
                        return cur;
                    }
                };
    }
}

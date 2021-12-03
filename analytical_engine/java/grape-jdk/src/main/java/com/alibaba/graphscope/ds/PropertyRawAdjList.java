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
@FFITypeAlias(CppClassName.PROPERTY_RAW_ADJ_LIST)
public interface PropertyRawAdjList<VID_T> extends FFIPointer, CXXPointer {
    @FFINameAlias("begin")
    PropertyNbrUnit<VID_T> begin();

    @FFINameAlias("end")
    PropertyNbrUnit<VID_T> end();

    @FFINameAlias("Size")
    int size();

    @FFINameAlias("Empty")
    boolean empty();

    default Iterable<PropertyNbrUnit<VID_T>> iterator() {
        return () ->
                new Iterator<PropertyNbrUnit<VID_T>>() {
                    PropertyNbrUnit<VID_T> cur = begin().moveTo(begin().getAddress());
                    long endAddr = end().getAddress();
                    long elementSize = cur.elementSize();
                    long curAddr = cur.getAddress();

                    @Override
                    public boolean hasNext() {
                        return curAddr != endAddr;
                    }

                    @Override
                    public PropertyNbrUnit<VID_T> next() {
                        cur.setAddress(curAddr);
                        curAddr += elementSize;
                        return cur;
                    }
                };
    }
}

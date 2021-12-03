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

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.context.ffi.FFIVertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.fragment.EdgecutFragment;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Objects;

public abstract class VertexDataContext<FRAG_T extends EdgecutFragment, DATA_T> {
    private long ffiContextAddress;
    private FFIVertexDataContext<FRAG_T, DATA_T> ffiVertexDataContext;
    private FFIVertexDataContext.Factory factory;

    /**
     * Must be called by jni, to create ffi context.
     *
     * @param fragment fragment
     * @param dataClass the class obj for the data type.
     * @param includeOuter whether to include outer vertices or not.
     */
    protected void createFFIContext(FRAG_T fragment, Class<?> dataClass, boolean includeOuter) {
        // String fragmentTemplateStr = FFITypeFactory.getFFITypeName(fragment.getClass(), true);
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment);
        System.out.println("fragment: " + fragmentTemplateStr);
        String contextName =
                FFITypeFactoryhelper.makeParameterize(
                        CppClassName.VERTEX_DATA_CONTEXT,
                        fragmentTemplateStr,
                        FFITypeFactoryhelper.javaType2CppType(dataClass));
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFIVertexDataContext.class, contextName);
        ffiVertexDataContext = factory.create(fragment, includeOuter);
        ffiContextAddress = ffiVertexDataContext.getAddress();
        System.out.println(contextName + ", " + ffiContextAddress);
    }

    public GSVertexArray<DATA_T> data() {
        if (Objects.isNull(ffiVertexDataContext)) {
            return null;
        }
        return ffiVertexDataContext.data();
    }
}

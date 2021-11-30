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
import com.alibaba.graphscope.context.ffi.FFILabeledVertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Objects;

public abstract class LabeledVertexDataContext<OID_T, DATA_T> {

    private long ffiContextAddress;
    private FFILabeledVertexDataContext<ArrowFragment<OID_T>, DATA_T> ffiLabeledVertexDataContext;
    private FFILabeledVertexDataContext.Factory factory;

    /**
     * ust be called by jni, to create ffi context.
     *
     * @param fragment fragment query fragment.
     * @param oidClass dataClass class object for data type.
     * @param dataClass class object for data.
     */
    protected void createFFIContext(
            ArrowFragment<OID_T> fragment, Class<?> oidClass, Class<?> dataClass) {
        // System.out.println("fragment: " + FFITypeFactoryhelper.makeParameterize(ARROW_FRAGMENT,
        // FFITypeFactoryhelper.javaType2CppType(oidClass)));
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment);
        String contextName =
                FFITypeFactoryhelper.makeParameterize(
                        CppClassName.LABELED_VERTEX_DATA_CONTEXT,
                        fragmentTemplateStr,
                        FFITypeFactoryhelper.javaType2CppType(dataClass));
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFILabeledVertexDataContext.class, contextName);
        ffiLabeledVertexDataContext = factory.create(fragment, true);
        ffiContextAddress = ffiLabeledVertexDataContext.getAddress();
        System.out.println(contextName + ", " + ffiContextAddress);
    }

    public DATA_T getValue(Vertex<Long> vertex) {
        if (Objects.isNull(ffiLabeledVertexDataContext)) {
            return null;
        }
        return ffiLabeledVertexDataContext.getValue(vertex);
    }

    public StdVector<GSVertexArray<DATA_T>> data() {
        if (Objects.isNull(ffiLabeledVertexDataContext)) {
            return null;
        }
        return ffiLabeledVertexDataContext.data();
    }
}

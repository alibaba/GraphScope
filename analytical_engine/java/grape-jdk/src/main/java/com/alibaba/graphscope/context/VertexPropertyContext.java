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

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.column.DoubleColumn;
import com.alibaba.graphscope.column.IntColumn;
import com.alibaba.graphscope.column.LongColumn;
import com.alibaba.graphscope.context.ffi.FFIVertexPropertyContext;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** VertexPropertyContext only compatible with simple graph, i.e. ArrowProjectedFragment @FRAG_T */
public abstract class VertexPropertyContext<FRAG_T extends ArrowProjectedFragment> {
    private static Logger logger = LoggerFactory.getLogger(VertexPropertyContext.class.getName());
    private long ffiContextAddress;
    private FFIVertexPropertyContext<FRAG_T> ffiVertexPropertyContext;
    private FFIVertexPropertyContext.Factory factory;

    /**
     * Must be called by jni, to create ffi context.
     *
     * @param fragment querying fragment
     */
    protected void createFFIContext(FRAG_T fragment) {
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment);
        System.out.println("fragment str: " + fragmentTemplateStr);
        String contextName =
                FFITypeFactoryhelper.makeParameterize(
                        CppClassName.VERTEX_PROPERTY_CONTEXT, fragmentTemplateStr);
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFIVertexPropertyContext.class, contextName);
        ffiVertexPropertyContext = factory.create(fragment);
        ffiContextAddress = ffiVertexPropertyContext.getAddress();
        System.out.println(
                "create vertex property Context: " + contextName + "@" + ffiContextAddress);
    }

    public long addColumn(String str, ContextDataType contextDataType) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(str);
            return ffiVertexPropertyContext.addColumn(byteString, contextDataType);
        }
        logger.error("ffi vertex context empty ");
        return -1;
    }

    public DoubleColumn<FRAG_T> getDoubleColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getDoubleColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public DoubleColumn<FRAG_T> getDoubleColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getDoubleColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<FRAG_T> getIntColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getIntColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<FRAG_T> getIntColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getIntColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<FRAG_T> getLongColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getLongColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<FRAG_T> getLongColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getLongColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }
}

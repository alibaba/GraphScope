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

package com.alibaba.graphscope.stdcxx;

import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import java.io.OutputStream;

@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(
        value = {},
        system = {"ostream"})
@FFITypeAlias("std::ostream")
public interface OStream extends FFIPointer {
    /**
     * For `put(byte c)` we will generate a native method:
     *
     * <pre>{@code
     * public class OStreamImpl{
     *     public void put(byte c) { nativePut(getAddress(), c); }
     *     native void nativePut(long address, byte c);
     * }
     * }</pre>
     *
     * In the generated JNI cxx file:
     *
     * <pre>{@code
     * void Java_xxx_nativePut(JNIEnv*, jclass, jlong address, jbytec)
     * {
     *      std::ostream* obj = reinterpret_cast<std::ostream*>(address);
     *      obj->put(c);
     * }
     * }</pre>
     *
     * @param c byte
     */
    void put(byte c);

    default OutputStream outputStream() {
        return new OutputStream() {
            @Override
            public void write(int i) {
                OStream.this.put((byte) i);
            }

            public void write(byte[] buf, int begin, int length) {
                int end = begin + length;
                for (int i = begin; i < end; i++) {
                    OStream.this.put(buf[i]);
                }
            }
        };
    }
}

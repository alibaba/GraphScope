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

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.io.Closeable;

@Structure.FieldOrder({"buffer", "len", "error"})
public class FfiData extends Structure {
    public FfiData() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiData implements Structure.ByValue, Closeable {
        public byte[] getBytes() {
            if (buffer != null && len > 0) {
                byte[] bytes = new byte[len];
                for (int i = 0; i < len; ++i) {
                    bytes[i] = buffer.getByte(i);
                }
                return bytes;
            }
            return null;
        }

        @Override
        public void close() {
            setAutoSynch(false);
            IrCoreLibrary.INSTANCE.destroyJobBuffer(this);
        }
    }

    public Pointer buffer;
    public int len;
    public FfiError.ByValue error;
}

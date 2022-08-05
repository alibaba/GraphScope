/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.store.jna;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.io.Closeable;
import java.io.IOException;

@Structure.FieldOrder({"success", "hasDdl", "errMsg", "data", "len"})
public class JnaResponse extends Structure implements Closeable {

    public int success;
    public int hasDdl;
    public String errMsg;
    public Pointer data;
    public int len;

    public boolean success() {
        return success == 1;
    }

    public boolean hasDdl() {
        return hasDdl == 1;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public byte[] getData() {
        if (this.data != null) {
            return this.data.getByteArray(0, this.len);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        setAutoSynch(false);
        GraphLibrary.INSTANCE.dropJnaResponse(this);
    }
}

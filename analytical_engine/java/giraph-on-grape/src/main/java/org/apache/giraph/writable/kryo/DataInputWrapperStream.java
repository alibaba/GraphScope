/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.writable.kryo;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Thin wrapper around DataInput so it can be used as an {@link java.io.InputStream}
 * <p>
 * For use with {@link com.esotericsoftware.kryo.io.Input}
 */
public class DataInputWrapperStream extends InputStream {

    /**
     * Wrapped DataInput object
     */
    private DataInput in;

    public void setDataInput(DataInput in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        try {
            return in.readByte() & 0xFF;
        } catch (EOFException e) {
            throw new RuntimeException(
                    "Chunked input should never read more than chunked output wrote", e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            in.readFully(b, off, len);
            return len;
        } catch (EOFException e) {
            throw new RuntimeException(
                    "Chunked input should never read more than chunked output wrote", e);
        }
    }
}

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
package com.alibaba.graphscope.utils;

/**
 * UnsafeByteArrayInputStream
 * <p>
 * This stream now extends com.esotericsoftware.kryo.io.Input so that kryo serialization can
 * directly read from this stream without using an additional buffer, providing a faster
 * serialization.
 * <p>
 * Users of this class has to explicitly close the stream to avoid style check errors even though
 * close is no-op when the underlying stream is not set.
 */
public class UnsafeByteArrayInputStream extends UnsafeArrayReads {

    /**
     * Constructor
     *
     * @param buf Buffer to read from
     */
    public UnsafeByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Constructor.
     *
     * @param buf    Buffer to read from
     * @param offset Offsetin the buffer to start reading from
     * @param length Max length of the buffer to read
     */
    public UnsafeByteArrayInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }
}

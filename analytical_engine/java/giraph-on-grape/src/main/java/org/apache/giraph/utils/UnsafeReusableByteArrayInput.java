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

package org.apache.giraph.utils;

import com.alibaba.graphscope.utils.UnsafeArrayReads;

/**
 * UnsafeReusableByteArrayInput is a data structure to read from a
 * byte buffer with a read pointer that can be moved to desired location
 */
public class UnsafeReusableByteArrayInput extends UnsafeArrayReads {

    /**
     * Default Constructor
     */
    public UnsafeReusableByteArrayInput() {
        super(null, 0, 0);
    }

    /**
     * Initialize the object with all required parameters
     *
     * @param buf byte buffer
     * @param offset offset in the buffer
     * @param length length of the valid data
     */
    public void initialize(byte[] buf, int offset, int length) {
        this.buffer = buf;
        this.position = offset;
        this.limit = length;
    }
}

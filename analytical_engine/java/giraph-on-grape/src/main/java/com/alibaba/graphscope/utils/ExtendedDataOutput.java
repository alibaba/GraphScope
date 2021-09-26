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

import java.io.DataOutput;

/**
 * Add a few features to data output
 */
public interface ExtendedDataOutput extends DataOutput {
    /**
     * Ensure that backing byte structure has at least minSize
     * additional bytes
     *
     * @param minSize additional size required
     */
    void ensureWritable(int minSize);

    /**
     * Skip some number of bytes.
     *
     * @param  bytesToSkip Number of bytes to skip
     */
    void skipBytes(int bytesToSkip);

    /**
     * In order to write a size as a first part of an data output, it is
     * useful to be able to write an int at an arbitrary location in the stream
     *
     * @param pos Byte position in the output stream
     * @param value Value to write
     */
    void writeInt(int pos, int value);

    /**
     * Get the position in the output stream
     *
     * @return Position in the output stream
     */
    int getPos();

    /**
     * Get the internal byte array (if possible), read-only
     *
     * @return Internal byte array (do not modify)
     */
    byte[] getByteArray();

    /**
     * Copies the internal byte array
     *
     * @return Copied byte array
     */
    byte[] toByteArray();

    /**
     * Return a copy of slice of byte array
     *
     * @param offset offset of array
     * @param length length of slice
     * @return byte array
     */
    byte[] toByteArray(int offset, int length);

    /**
     * Clears the buffer
     */
    void reset();
}

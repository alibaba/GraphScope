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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Stores vertex ids and data associated with a vertex
 *
 * @param <I> vertexId type parameter
 * @param <T> vertexData type parameter
 */
public interface VertexIdData<I extends WritableComparable, T>
    extends ImmutableClassesGiraphConfigurable, Writable {
    /**
     * Create a new data object.
     *
     * @return Newly-created data object.
     */
    T createData();

    /**
     * Write a data object to an {@link ExtendedDataOutput}.
     *
     * @param out  {@link ExtendedDataOutput}
     * @param data Data object to write
     * @throws IOException
     */
    void writeData(ExtendedDataOutput out, T data) throws IOException;

    /**
     * Read a data object's fields from an {@link ExtendedDataInput}.
     *
     * @param in   {@link ExtendedDataInput}
     * @param data Data object to fill in-place
     * @throws IOException
     */
    void readData(ExtendedDataInput in, T data) throws IOException;

    /**
     * Initialize the inner state. Must be called before {@code add()} is
     * called.
     */
    void initialize();

    /**
     * Initialize the inner state, with a known size. Must be called before
     * {@code add()} is called.
     *
     * @param expectedSize Number of bytes to be expected
     */
    void initialize(int expectedSize);

    /**
     * Add a vertex id and data pair to the collection.
     *
     * @param vertexId Vertex id
     * @param data Data
     */
    void add(I vertexId, T data);

    /**
     * Add a serialized vertex id and data.
     *
     * @param serializedId The bye array which holds the serialized id.
     * @param idPos The end position of the serialized id in the byte array.
     * @param data Data
     */
    void add(byte[] serializedId, int idPos, T data);

    /**
     * Get the number of bytes used.
     *
     * @return Bytes used
     */
    int getSize();

    /**
     * Get the size of this object in serialized form.
     *
     * @return The size (in bytes) of the serialized object
     */
    int getSerializedSize();

    /**
     * Check if the list is empty.
     *
     * @return Whether the list is empty
     */
    boolean isEmpty();

    /**
     * Clear the list.
     */
    void clear();

    /**
     * Get an iterator over the pairs.
     *
     * @return Iterator
     */
    VertexIdDataIterator<I, T> getVertexIdDataIterator();
}

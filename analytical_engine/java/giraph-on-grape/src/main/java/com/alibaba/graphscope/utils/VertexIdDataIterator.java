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

import org.apache.hadoop.io.WritableComparable;

/**
 * Special iterator that reuses vertex ids and data objects so that the lifetime of the object is
 * only until next() is called.
 * <p>
 * Vertex id ownership can be released if desired through releaseCurrentVertexId().  This
 * optimization allows us to cut down on the number of objects instantiated and garbage collected.
 *
 * @param <I> vertexId type parameter
 * @param <T> vertexData type parameter
 */
public interface VertexIdDataIterator<I extends WritableComparable, T> extends VertexIdIterator<I> {

    /**
     * Get the current data.
     *
     * @return Current data
     */
    T getCurrentData();

    /**
     * Get serialized size of current data
     *
     * @return serialized size of data
     */
    int getCurrentDataSize();

    /**
     * Release the current data object.
     *
     * @return Released data object
     */
    T releaseCurrentData();
}

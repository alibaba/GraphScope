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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * VertexIdMessages
 *
 * @param <I> vertexId type parameter
 * @param <M> message type parameter
 */
public interface VertexIdMessages<I extends WritableComparable, M extends Writable>
        extends VertexIdData<I, M> {

    /**
     * Get specialized iterator that will instantiate the vertex id and message of this object.  It
     * will only produce message bytes, not actual messages and expects a different encoding.
     *
     * @return Special iterator that reuses vertex ids (unless released) and copies message bytes
     */
    VertexIdMessageBytesIterator<I, M> getVertexIdMessageBytesIterator();

    /**
     * Get specialized iterator that will instiantiate the vertex id and message of this object.
     *
     * @return Special iterator that reuses vertex ids and messages unless specified
     */
    VertexIdMessageIterator<I, M> getVertexIdMessageIterator();
}

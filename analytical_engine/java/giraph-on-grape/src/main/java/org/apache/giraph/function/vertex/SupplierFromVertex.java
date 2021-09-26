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
package org.apache.giraph.function.vertex;

import java.io.Serializable;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Function:
 * (vertex) -&gt; T
 *
 * A class that can supply objects of a single type, when given a vertex.
 *
 * (doesn't extend Function&lt;Vertex&lt;I, V, E&gt;, T&gt;,
 * because of different method names)
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <T> Result type
 */
@SuppressWarnings("rawtypes")
public interface SupplierFromVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, T> extends Serializable {
  /**
   * Retrieves an instance of the appropriate type, given a vertex.
   * The returned object may or may not be a new instance,
   * depending on the implementation.
   *
   * @param vertex Vertex
   * @return result
   */
  T get(Vertex<I, V, E> vertex);
}

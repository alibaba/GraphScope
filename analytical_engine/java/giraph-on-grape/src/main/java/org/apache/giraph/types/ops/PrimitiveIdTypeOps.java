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
package org.apache.giraph.types.ops;

import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.types.ops.collections.WritableWriter;



/**
 * Additional type operations to TypeOps for types that can be IDs,
 * and so can be used as keys in maps and values in sets.
 *
 * Using any of the provided operations should lead to no boxing/unboxing.
 *
 * Useful generic wrappers to fastutil libraries are provided,
 * so that you can look at them generically.
 *
 * @param <T> Type
 */
public interface PrimitiveIdTypeOps<T> extends PrimitiveTypeOps<T> {
  // primitive collections

  /**
   * Create BasicSet of type T.
   * @return BasicSet
   */
  BasicSet<T> createOpenHashSet();

  /**
   * Create BasicSet of type T, given capacity.
   * @param capacity Capacity
   * @return BasicSet
   */
  BasicSet<T> createOpenHashSet(long capacity);

  /**
   * Create Basic2ObjectMap with key type T.
   * Values are represented as object, even if they can be primitive.
   *
   * You can pass null as valueWriter,
   * but readFields/write will throw an Exception, if called.
   *
   * @param valueWriter Writer of values
   * @param <V> Type of values in the map
   * @return Basic2ObjectMap
   */
  <V> Basic2ObjectMap<T, V> create2ObjectOpenHashMap(
      WritableWriter<V> valueWriter);

  /**
   * Create Basic2ObjectMap with key type T, given capacity.
   * Values are represented as object, even if they can be primitive.
   *
   * You can pass null as valueWriter,
   * but readFields/write will throw an Exception, if called.
   *
   * @param capacity Capacity
   * @param valueWriter Writer of values
   * @param <V> Type of values in the map
   * @return Basic2ObjectMap
   */
  <V> Basic2ObjectMap<T, V> create2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter);
}

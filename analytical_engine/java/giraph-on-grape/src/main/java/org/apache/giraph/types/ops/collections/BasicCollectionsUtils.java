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
package org.apache.giraph.types.ops.collections;

import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap.BasicObject2ObjectOpenHashMap;
import org.apache.hadoop.io.Writable;

/**
 * Utility functions for constructing basic collections
 */
public class BasicCollectionsUtils {
  /** No instances */
  private BasicCollectionsUtils() { }

  /**
   * Construct OpenHashMap with primitive keys.
   *
   * @param <I> Vertex id type
   * @param <V> Value type
   * @param idClass Class type
   * @return map
   */
  public static <I extends Writable, V>
  Basic2ObjectMap<I, V> create2ObjectMap(Class<I> idClass) {
    return create2ObjectMap(idClass, null, null);
  }

  /**
   * Construct OpenHashMap with primitive keys.
   *
   * If keyWriter/valueWriter are not provided,
   * readFields/write will throw an Exception, if called.
   *
   * @param <I> Vertex id type
   * @param <V> Value type
   * @param idClass Class type
   * @param keyWriter writer for keys
   * @param valueWriter writer for values
   * @return map
   */
  public static <I extends Writable, V>
  Basic2ObjectMap<I, V> create2ObjectMap(
    Class<I> idClass,
    WritableWriter<I> keyWriter,
    WritableWriter<V> valueWriter
  ) {
    PrimitiveIdTypeOps<I> idTypeOps = TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(
      idClass
    );
    if (idTypeOps != null) {
      return idTypeOps.create2ObjectOpenHashMap(valueWriter);
    } else {
      return new BasicObject2ObjectOpenHashMap<>(keyWriter, valueWriter);
    }
  }
}

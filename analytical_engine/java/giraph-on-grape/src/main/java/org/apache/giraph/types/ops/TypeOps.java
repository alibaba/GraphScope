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


/**
 * Type operations, allowing working generically with mutable types,
 * but still having efficient code.
 * For example, by reducing object allocation via reuse.
 *
 * Use enum singleton pattern, having single enum value - INSTANCE.
 * Serialization code depends on implementations being enums.
 *
 * @param <T> Type
 */
public interface TypeOps<T> {
  /**
   * Class object for generic type T.
   * @return Class&lt;T&gt; object
   */
  Class<T> getTypeClass();
  /**
   * Create new instance of type T.
   * @return new instance
   */
  T create();
  /**
   * Create a copy of passed object
   * @param from Object to copy
   * @return Copy
   */
  T createCopy(T from);
  /**
   * Copies the value from the second argument into the first.
   * @param to Object into which the value should be copied
   * @param from Value of object to be copied
   */
  void set(T to, T from);
}

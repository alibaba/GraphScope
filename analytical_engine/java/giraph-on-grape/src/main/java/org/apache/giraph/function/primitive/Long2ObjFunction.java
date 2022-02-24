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
package org.apache.giraph.function.primitive;

import java.io.Serializable;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/**
 * Primitive specialization of Function:
 * (long) -&gt; T
 *
 * @param <T> Result type
 */
public interface Long2ObjFunction<T> extends Serializable {
  /**
   * Returns the result of applying this function to given {@code input}.
   *
   * The returned object may or may not be a new instance,
   * depending on the implementation.
   *
   * @param input input
   * @return result
   */
  T apply(long input);
}

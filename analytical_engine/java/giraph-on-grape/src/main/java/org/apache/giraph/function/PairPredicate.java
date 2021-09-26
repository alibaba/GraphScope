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
package org.apache.giraph.function;

import java.io.Serializable;

/**
 * Function:
 * (T) -&gt; boolean
 *
 * @param <T1> First argument type
 * @param <T2> Second argument type
 */
public interface PairPredicate<T1, T2> extends Serializable {
  /**
   * Returns the result of applying this predicate to
   * {@code input1} and {@code input2}.
   *
   * @param input1 first input
   * @param input2 second input
   * @return result
   */
  boolean apply(T1 input1, T2 input2);
}

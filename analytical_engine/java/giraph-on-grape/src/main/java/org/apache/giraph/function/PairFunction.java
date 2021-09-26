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
 * (F1, F2) -&gt; T
 *
 * @param <F1> First argument type
 * @param <F2> Second argument type
 * @param <T> Result type
 */
public interface PairFunction<F1, F2, T> extends Serializable {
  /**
   * Returns the result of applying this function to given
   * {@code input1} and {@code input2}.
   *
   * The returned object may or may not be a new instance,
   * depending on the implementation.
   *
   * @param input1 first input
   * @param input2 second input
   * @return result
   */
  T apply(F1 input1, F2 input2);
}

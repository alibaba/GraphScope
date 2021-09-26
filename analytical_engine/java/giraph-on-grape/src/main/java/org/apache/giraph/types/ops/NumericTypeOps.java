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
 * Numeric type operations, allowing working generically with types,
 * but still having efficient code.
 *
 * Using any of the provided operations should lead to no boxing/unboxing.
 *
 * @param <T> Type
 */
public interface NumericTypeOps<T> extends TypeOps<T> {
  /**
   * Value of zero
   * @return New object with value of zero
   */
  T createZero();
  /**
   * Value of one
   * @return New object with value of one
   */
  T createOne();

  /**
   * Minimal negative value representable via current type.
   * Negative infinity for floating point numbers.
   * @return New object with min negative value
   */
  T createMinNegativeValue();
  /**
   * Maximal positive value representable via current type.
   * Positive infinity for floating point numbers.
   * @return New object with max positive value
   */
  T createMaxPositiveValue();


  /**
   * value+=adder
   *
   * @param value Value to modify
   * @param increment Increment
   */
  void plusInto(T value, T increment);
  /**
   * value*=multiplier
   *
   * @param value Value to modify
   * @param multiplier Multiplier
   */
  void multiplyInto(T value, T multiplier);

  /**
   * -value
   * @param value Value to negate
   */
  void negate(T value);

  /**
   * Compare two values
   *
   * @param value1 First value
   * @param value2 Second value
   * @return 0 if values are equal, negative value if value1&lt;value2 and
   *         positive value if value1&gt;value2
   */
  int compare(T value1, T value2);
}

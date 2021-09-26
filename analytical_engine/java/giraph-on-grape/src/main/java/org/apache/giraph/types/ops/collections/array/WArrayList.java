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
package org.apache.giraph.types.ops.collections.array;
import org.apache.giraph.types.ops.collections.WCollection;

/**
 * Array list over mutable elements, which are probably
 * internally stored differently/efficiently, and are accessed
 * through methods providing "return" value.
 *
 * @param <T> Element type
 */
public interface WArrayList<T> extends WCollection<T> {
  /**
   * Sets the size of this
   *
   * <P>
   * If the specified size is smaller than the current size,
   * the last elements are discarded.
   * Otherwise, they are filled with 0/<code>null</code>/<code>false</code>.
   *
   * @param newSize the new size.
   */
  void size(int newSize);
  /**
   * Trims this array list so that the capacity is equal to the size.
   *
   * @see java.util.ArrayList#trimToSize()
   */
  void trim();
  /**
   * Pop value from the end of the array, storing it into 'to' argument
   * @param to Object to store value into
   */
  void popIntoW(T to);
  /**
   * Get element at given index in the array, storing it into 'to' argument
   * @param index Index
   * @param to Object to store value into
   */
  void getIntoW(int index, T to);
  /**
   * Set element at given index in the array
   * @param index Index
   * @param value Value
   */
  void setW(int index, T value);
  /**
   * Sets given range of elements to a specified value.
   *
   * @param from From index (inclusive)
   * @param to To index (exclusive)
   * @param value Value
   */
  void fillW(int from, int to, T value);
  /** Sort the array in ascending order */
  void sort();
}

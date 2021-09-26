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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.hadoop.io.Writable;

/**
 * Collection over mutable elements, which are probably
 * internally stored differently/efficiently, and are accessed
 * through methods providing "return" value.
 *
 * @param <T> Element type
 */
public interface WCollection<T> extends Writable {
  /** Removes all of the elements from this  */
  void clear();
  /**
   * Number of elements in this list
   * @return size
   */
  int size();
  /**
   * Capacity of currently allocated memory
   * @return capacity
   */
  int capacity();
  /**
   * Forces allocated memory to hold exactly N values
   * @param n new capacity
   */
  void setCapacity(int n);
  /**
   * Add value to the collection
   * @param value Value
   */
  void addW(T value);
  /**
   * TypeOps for type of elements this object holds
   * @return TypeOps
   */
  PrimitiveTypeOps<T> getElementTypeOps();
  /**
   * Fast iterator over collection objects, which doesn't allocate new
   * element for each returned element, and can be iterated multiple times
   * using reset().
   *
   * Object returned by next() is only valid until next() is called again,
   * because it is reused.
   *
   * @return RessettableIterator
   */
  ResettableIterator<T> fastIteratorW();
  /**
   * Fast iterator over collection objects, which doesn't allocate new
   * element for each returned element, and can be iterated multiple times
   * using reset().
   *
   * Each call to next() populates passed value.
   *
   * @param iterationValue Value that call to next() will populate
   * @return RessettableIterator
   */
  ResettableIterator<T> fastIteratorW(T iterationValue);
  /**
   * Traverse all elements of the collection, calling given function on each
   * element. Passed values are valid only during the call to the passed
   * function, so data needs to be consumed during the function.
   *
   * @param f Function to call on each element.
   */
  void fastForEachW(Consumer<T> f);
  /**
   * Traverse all elements of the collection, calling given function on each
   * element, or until predicate returns false.
   * Passed values are valid only during the call to the passed
   * function, so data needs to be consumed during the function.
   *
   * @param f Function to call on each element.
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   */
  boolean fastForEachWhileW(Predicate<T> f);
  /**
   * Write elements to the DataOutput stream, without the size itself.
   * Can be read back using readElements function.
   *
   * @param out Data output
   */
  void writeElements(DataOutput out) throws IOException;
  /**
   * Read elements from DataInput stream, with passing the size instead
   * reading it from the stream.
   *
   * @param in Data Input
   * @param size Number of elements
   */
  void readElements(DataInput in, int size) throws IOException;
}

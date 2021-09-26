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

import java.util.NoSuchElementException;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.types.ops.collections.ResettableIterator;

/**
 * Private utilities class
 *
 * (needed since Java 7 has no default methods on interfaces,
 * and we need to extend appropriate fastutil classes)
 */
class WArrayListPrivateUtils {
  /** Hide constructor */
  private WArrayListPrivateUtils() { }

  /**
   * Fast iterator over collection objects, which doesn't allocate new
   * element for each returned element, and can be iterated multiple times
   * using reset().
   *
   * Object returned by next() is only valid until next() is called again,
   * because it is reused.
   *
   * @param list Collection to iterate over
   * @param iterationValue reusable iteration value
   * @return RessettableIterator
   * @param <T> Element type
   */
  static <T> ResettableIterator<T> fastIterator(
      final WArrayList<T> list, final T iterationValue) {
    return new ResettableIterator<T>() {
      private int pos;

      @Override
      public boolean hasNext() {
        return pos < list.size();
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        list.getIntoW(pos, iterationValue);
        pos++;
        return iterationValue;
      }

      @Override
      public void reset() {
        pos = 0;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Traverse all elements of the collection, calling given function on each
   * element. Passed values are valid only during the call to the passed
   * function, so data needs to be consumed during the function.
   *
   * @param list Collection to iterate over
   * @param f Function to call on each element.
   * @param iterationValue reusable iteration value
   * @param <T> Element type
   */
  static <T> void fastForEach(
      WArrayList<T> list, Consumer<T> f, T iterationValue) {
    for (int i = 0; i < list.size(); ++i) {
      list.getIntoW(i, iterationValue);
      f.apply(iterationValue);
    }
  }

  /**
   * Traverse all elements of the collection, calling given function on each
   * element, or until predicate returns false.
   * Passed values are valid only during the call to the passed
   * function, so data needs to be consumed during the function.
   *
   * @param list Collection to iterate over
   * @param f Function to call on each element.
   * @param iterationValue reusable iteration value
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   * @param <T> Element type
   */
  static <T> boolean fastForEachWhile(
      WArrayList<T> list, Predicate<T> f, T iterationValue) {
    for (int i = 0; i < list.size(); ++i) {
      list.getIntoW(i, iterationValue);
      if (!f.apply(iterationValue)) {
        return false;
      }
    }
    return true;
  }
}

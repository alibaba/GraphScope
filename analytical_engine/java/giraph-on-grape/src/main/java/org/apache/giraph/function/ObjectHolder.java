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

/**
 * Default object holder, intermediary between producers and consumers.
 *
 * Holds value in memory, so can be used only when producer and consumer
 * are in the same context.
 *
 * Useful when value is set on the master, and later read in block logic
 * (RepeatUntilBlock), or in a different Piece, either on worker or master.
 * If it is read within the same piece - just use local field.
 *
 * @param <T> Type of object to hold.
 */
public class ObjectHolder<T> implements Supplier<T>, Consumer<T> {
  /** value */
  private T value;

  /**
   * Constructor
   * @param value initial value
   */
  public ObjectHolder(T value) {
    this.value = value;
  }

  /** Constructor */
  public ObjectHolder() {
  }

  @Override
  public T get() {
    return value;
  }

  @Override
  public void apply(T value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return getClass() + " [value=" + value + "]";
  }
}

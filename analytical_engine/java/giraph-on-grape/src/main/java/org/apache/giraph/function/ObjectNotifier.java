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

import java.util.ArrayList;
import java.util.List;

/**
 * Allows multiple Consumers to be combined into a single Consumer, by having
 * them all subscribe to this object, and then using this object as a group
 * Consumer.
 *
 * @param <T> Argument type
 */
public class ObjectNotifier<T> implements Consumer<T>, Notifier<T> {
  /** List of subscribers */
  private final List<Consumer<T>> subscribers = new ArrayList<>();

  @Override
  public void apply(T value) {
    for (Consumer<T> consumer : subscribers) {
      consumer.apply(value);
    }
  }

  @Override
  public void subscribe(Consumer<T> consumer) {
    subscribers.add(consumer);
  }

  @Override
  public String toString() {
    return getClass() + " [subscribers=" + subscribers + "]";
  }
}

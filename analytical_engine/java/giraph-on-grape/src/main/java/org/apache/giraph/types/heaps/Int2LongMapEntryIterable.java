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

package org.apache.giraph.types.heaps;

import it.unimi.dsi.fastutil.ints.Int2LongMap;;
import it.unimi.dsi.fastutil.objects.ObjectIterable;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Iterable which has its size and
 * ObjectIterator&lt;Int2LongMap.Entry&gt;
 */
public interface Int2LongMapEntryIterable
    extends ObjectIterable<Int2LongMap.Entry> {
  /**
   * Get the iterator. Not thread-safe and reuses iterator object,
   * so you can't have several iterators at the same time.
   *
   * @return Iterator
   */
  ObjectIterator<Int2LongMap.Entry> iterator();

  /**
   * Get the size of this iterable
   *
   * @return Size
   */
  int size();
}

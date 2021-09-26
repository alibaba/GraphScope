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

import it.unimi.dsi.fastutil.ints.AbstractInt2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.giraph.function.primitive.pairs.IntFloatConsumer;
import org.apache.giraph.function.primitive.pairs.IntFloatPredicate;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/**
 * Min heap which holds (int key, float value) pairs with
 * the largest values as its elements, up to the given maximum number of
 * elements.
 *
 * When multiple elements with same values are added and there is no space for
 * all of them in the heap, the one with larger keys will be kept in the heap.
 *
 * You can remove a pair with the minimum value currently in the heap.
 */
public class FixedCapacityIntFloatMinHeap
    implements Int2FloatMapEntryIterable {
  /** Keys in the heap */
  private final int[] keys;
  /** Values in the heap */
  private final float[] values;
  /** Number of elements currently in the heap */
  private int size;
  /** Capacity of the heap */
  private final int capacity;
  /** Reusable iterator instance */
  private final IteratorImpl iterator;

  /**
   * Initialize the heap with desired capacity
   *
   * @param capacity Capacity
   */
  public FixedCapacityIntFloatMinHeap(int capacity) {
    keys = new int[capacity];
    values = new float[capacity];
    size = 0;
    this.capacity = capacity;
    iterator = new IteratorImpl();
  }

  /** Clear the heap */
  public void clear() {
    size = 0;
  }

  /**
   * Add a key value pair
   *
   * @param key   Key
   * @param value Value
   */
  public void add(int key, float value) {
    if (capacity == 0 ||
        (size == capacity && compare(keys[0], values[0], key, value) >= 0)) {
      // If the heap is full and smallest element in it is not smaller
      // than value, do nothing
      return;
    }
    int position;
    if (size < capacity) {
      // If the heap is not full, increase its size and find the position for
      // new element (up-heap search)
      position = size;
      size++;
      while (position > 0) {
        int parent = (position - 1) >> 1;
        if (compare(keys[parent], values[parent], key, value) < 0) {
          break;
        }
        values[position] = values[parent];
        keys[position] = keys[parent];
        position = parent;
      }
    } else {
      // If the heap is full, remove element from the root and find the position
      // for new element (down-heap search)
      position = removeRootAndFindPosition(key, value);
    }
    // Fill position with key value pair
    keys[position] = key;
    values[position] = value;
  }

  /**
   * @return Key corresponding to the minimum value currently in the heap
   * @throws NoSuchElementException if the heap is empty.
   */
  public int getMinKey() {
    if (size() > 0) {
      return keys[0];
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * @return Minimum value currently in the heap
   * @throws NoSuchElementException if the heap is empty.
   */
  public float getMinValue() {
    if (size() > 0) {
      return values[0];
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * Removes the (key, value) pair that corresponds to the minimum value
   * currently in the heap.
   */
  public void removeMin() {
    if (size() > 0) {
      size--;
      int position = removeRootAndFindPosition(keys[size], values[size]);
      keys[position] = keys[size];
      values[position] = values[size];
    }
  }

  /**
   * Comapre two (key, value) entries
   *
   * @param key1   First key
   * @param value1 First value
   * @param key2   Second key
   * @param value2 Second value
   * @return 0 if entries are equal, &lt; 0 if first entry is smaller than the
   * second one, and &gt; 0 if first entry is larger than the second one
   */
  protected int compare(int key1, float value1,
      int key2, float value2) {
    int t = Float.compare(value1, value2);
    return (t == 0) ? Integer.compare(key1, key2) : t;
  }

  @Override
  public ObjectIterator<Int2FloatMap.Entry> iterator() {
    iterator.reset();
    return iterator;
  }

  @Override
  public int size() {
    return size;
  }

  /**
   * Check if the heap is empty
   *
   * @return True iff the heap is empty
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Get capacity of the heap
   *
   * @return Heap capacity
   */
  public int getCapacity() {
    return capacity;
  }

  /**
   * Serializes an object into data output.
   *
   * @param heap Object instance to serialize
   * @param out  Data output
   * @throws java.io.IOException
   */
  public static void write(FixedCapacityIntFloatMinHeap heap,
      DataOutput out) throws IOException {
    out.writeInt(heap.capacity);
    out.writeInt(heap.size);
    for (int i = 0; i < heap.size(); i++) {
      out.writeInt(heap.keys[i]);
      out.writeFloat(heap.values[i]);
    }
  }

  /**
   * Deserializes an object from data input.
   *
   * @param heap Object to reuse if possible
   * @param in   Data input
   * @return FixedCapacityIntFloatMinHeap deserialized from data input.
   * @throws IOException
   */
  public static FixedCapacityIntFloatMinHeap read(
      FixedCapacityIntFloatMinHeap heap, DataInput in)
      throws IOException {
    int capacity = in.readInt();
    if (heap == null || heap.capacity != capacity) {
      heap = new FixedCapacityIntFloatMinHeap(capacity);
    } else {
      heap.clear();
    }
    heap.size = in.readInt();
    for (int i = 0; i < heap.size; i++) {
      heap.keys[i] = in.readInt();
      heap.values[i] = in.readFloat();
    }
    return heap;
  }

  /**
   * Takes a (key, value) pair, removes the root of the heap, and finds
   * a position where the pair can be inserted.
   *
   * @param key   Key
   * @param value Value
   * @return Position in the heap where the (key, value) pair can be inserted
   * while preserving the heap property.
   */
  private int removeRootAndFindPosition(int key, float value) {
    int position = 0;
    while (position < size) {
      // Find the left child
      int minChild = (position << 1) + 1;
      // Compare the left and the right child values - find the smaller one
      if (minChild + 1 < size &&
          compare(keys[minChild + 1], values[minChild + 1],
              keys[minChild], values[minChild]) < 0) {
        minChild++;
      }
      if (minChild >= size || compare(keys[minChild], values[minChild],
          key, value) >= 0) {
        break;
      }
      keys[position] = keys[minChild];
      values[position] = values[minChild];
      position = minChild;
    }
    return position;
  }

  /**
   * Traverse all elements of the heap, calling given function on each element.
   *
   * @param f Function to call on each element.
   */
  public void forEachIntFloat(IntFloatConsumer f) {
    for (int i = 0; i < size(); ++i) {
      f.apply(keys[i], values[i]);
    }
  }

  /**
   * Traverse all elements of the heap, calling given function on each element,
   * or until predicate returns false.
   *
   * @param f Function to call on each element.
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   */
  public boolean forEachWhileIntFloat(IntFloatPredicate f) {
    for (int i = 0; i < size(); ++i) {
      if (!f.apply(keys[i], values[i])) {
        return false;
      }
    }
    return true;
  }

  /** Iterator for FixedCapacityIntFloatMinHeap */
  private class IteratorImpl implements ObjectIterator<Int2FloatMap.Entry> {
    /** Reusable entry */
    private final MutableEntry entry = new MutableEntry();
    /** Current index */
    private int index;

    /** Reset the iterator so it can be reused */
    public void reset() {
      index = -1;
    }

    @Override
    public boolean hasNext() {
      return index < size - 1;
    }

    @Override
    public Int2FloatMap.Entry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      index++;
      entry.setIntKey(keys[index]);
      entry.setFloatValue(values[index]);
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove() shouldn't be called");
    }

    @Override
    public int skip(int i) {
      throw new UnsupportedOperationException("skip(int) shouldn't be called");
    }
  }

  /** Helper mutable Entry class */
  private static class MutableEntry extends AbstractInt2FloatMap.BasicEntry {
    /** Default constructor */
    private MutableEntry() {
      super(0, 0);
    }

    /**
     * Set key
     *
     * @param key Key to set
     */
    private void setIntKey(int key) {
      this.key = key;
    }

    /**
     * Set value
     *
     * @param value Value to set
     */
    private void setFloatValue(float value) {
      this.value = value;
    }
  }
}

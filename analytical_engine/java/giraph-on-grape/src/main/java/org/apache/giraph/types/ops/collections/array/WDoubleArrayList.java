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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.DoubleConsumer;
import org.apache.giraph.function.primitive.DoublePredicate;
import org.apache.giraph.types.ops.DoubleTypeOps;
import org.apache.giraph.types.ops.PrimitiveTypeOps;
import org.apache.giraph.types.ops.collections.ResettableIterator;
import org.apache.giraph.types.ops.collections.WDoubleCollection;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.utils.Varint;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.doubles.DoubleList;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/**
 * Writable extension of DoubleArrayList, as well as
 * DoubleWritable implementation of WArrayList.
 */
public class WDoubleArrayList
    extends DoubleArrayList
    implements WArrayList<DoubleWritable>, WDoubleCollection {
  /**
   * Creates a new array list with {@link #DEFAULT_INITIAL_CAPACITY} capacity.
   */
  public WDoubleArrayList() {
    super();
  }

  /**
   * Creates a new array list with given capacity.
   *
   * @param capacity the initial capacity of the array list (may be 0).
   */
  public WDoubleArrayList(int capacity) {
    super(capacity);
  }

  /**
   * Creates a new array list and fills it with a given type-specific
   * collection.
   *
   * @param c a type-specific collection that will be used to fill the array
   *          list.
   */
  public WDoubleArrayList(DoubleCollection c) {
    super(c);
  }

  /**
   * Creates a new array list and fills it with a given type-specific list.
   *
   * @param l a type-specific list that will be used to fill the array list.
   */
  public WDoubleArrayList(DoubleList l) {
    super(l);
  }

  @Override
  public PrimitiveTypeOps<DoubleWritable> getElementTypeOps() {
    return DoubleTypeOps.INSTANCE;
  }

  @Override
  public int capacity() {
    return elements().length;
  }

  @Override
  public void setCapacity(int n) {
    if (n >= capacity()) {
      ensureCapacity(n);
    } else {
      trim(n);
    }
  }

  @Override
  public void addW(DoubleWritable value) {
    add(value.get());
  }

  @Override
  public void getIntoW(int index, DoubleWritable to) {
    to.set(getDouble(index));
  }

  @Override
  public void popIntoW(DoubleWritable to) {
    to.set(popDouble());
  }

  @Override
  public void setW(int index, DoubleWritable value) {
    set(index, value.get());
  }

  @Override
  public void fillW(int from, int to, DoubleWritable value) {
    if (to > size()) {
      throw new ArrayIndexOutOfBoundsException(
          "End index (" + to + ") is greater than array length (" +
              size() + ")");
    }
    Arrays.fill(elements(), from, to, value.get());
  }

  @Override
  public ResettableIterator<DoubleWritable> fastIteratorW() {
    return fastIteratorW(getElementTypeOps().create());
  }

  @Override
  public ResettableIterator<DoubleWritable> fastIteratorW(
      DoubleWritable iterationValue) {
    return WArrayListPrivateUtils.fastIterator(this, iterationValue);
  }

  @Override
  public void fastForEachW(Consumer<DoubleWritable> f) {
    WArrayListPrivateUtils.fastForEach(this, f, getElementTypeOps().create());
  }

  @Override
  public boolean fastForEachWhileW(Predicate<DoubleWritable> f) {
    return WArrayListPrivateUtils.fastForEachWhile(
        this, f, getElementTypeOps().create());
  }

  /**
   * Traverse all elements of the array list, calling given function on each
   * element, or until predicate returns false.
   *
   * @param f Function to call on each element.
   */
  public void forEachDouble(DoubleConsumer f) {
    for (int i = 0; i < size(); ++i) {
      f.apply(getDouble(i));
    }
  }

  /**
   * Traverse all elements of the array list, calling given function on each
   * element.
   *
   * @param f Function to call on each element.
   * @return true if the predicate returned true for all elements,
   *    false if it returned false for some element.
   */
  public boolean forEachWhileDouble(DoublePredicate f) {
    for (int i = 0; i < size(); ++i) {
      if (!f.apply(getDouble(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void sort() {
    DoubleArrays.quickSort(elements(), 0, size());
  }

  @Override
  public void writeElements(DataOutput out) throws IOException {
    for (int i = 0; i < size; i++) {
      out.writeDouble(a[i]);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Varint.writeUnsignedVarInt(size, out);
    writeElements(out);
  }

  @Override
  public void readElements(DataInput in, int size) throws IOException {
    this.size = size;
    resizeArrayForRead(size);
    for (int i = 0; i < size; i++) {
      a[i] = in.readDouble();
    }
  }

  /**
   * Resize array for reading given number of elements.
   * @param size Number of elements that will be read.
   */
  protected void resizeArrayForRead(int size) {
    if (size != a.length) {
      a = new double[size];
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    readElements(in, Varint.readUnsignedVarInt(in));
  }

  /**
   * Write a potentially null list to a DataOutput. Null list is written
   * equivalently as an empty list. Use WritableUtils.writeIfNotNullAndObject
   * if distinction is needed.
   *
   * @param list Array list to be written
   * @param out Data output
   */
  public static void writeOrNull(WDoubleArrayList list, DataOutput out)
      throws IOException {
    if (list == null) {
      Varint.writeUnsignedVarInt(0, out);
    } else {
      list.write(out);
    }
  }

  /**
   * Read array list from the DataInput stream into a newly created object.
   *
   * @param in Data input
   * @return New read array list object.
   */
  public static WDoubleArrayList readNew(DataInput in) throws IOException {
    int size = Varint.readSignedVarInt(in);
    WDoubleArrayList list = new WDoubleArrayList(size);
    list.readElements(in, size);
    return list;
  }

  /**
   * Variant of WDoubleArrayList that doesn't reallocate smaller backing
   * array on consecutive readFields/readElements calls, and so is suitable for
   * reusable use.
   * (and backing array will only grow on readFields/readElements calls)
   */
  public static class WReusableDoubleArrayList
      extends WDoubleArrayList {
    /** Constructor */
    public WReusableDoubleArrayList() {
      super();
    }

    /**
     * Constructor
     * @param capacity Capacity
     */
    public WReusableDoubleArrayList(int capacity) {
      super(capacity);
    }

    @Override
    protected void resizeArrayForRead(int size) {
      if (size > a.length) {
        a = new double[size];
      }
    }

    /**
     * Read array list from DataInput stream, into a given list if not null,
     * or creating a new list if given list is null.
     *
     * @param list Array list to be written
     * @param in Data input
     * @return Passed array list, or a new one if null is passed
     */
    public static WReusableDoubleArrayList readIntoOrCreate(
        WReusableDoubleArrayList list, DataInput in) throws IOException {
      int size = Varint.readUnsignedVarInt(in);
      if (list == null) {
        list = new WReusableDoubleArrayList(size);
      }
      list.readElements(in, size);
      return list;
    }
  }
}

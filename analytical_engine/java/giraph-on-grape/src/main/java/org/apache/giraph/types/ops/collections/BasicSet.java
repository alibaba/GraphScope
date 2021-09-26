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

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashBigSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * BasicSet with only basic set of operations.
 * All operations that return object T are returning reusable object,
 * which is modified after calling any other function.
 *
 * @param <T> Element type
 */
public interface BasicSet<T> extends Writable {
  /** Threshold for using OpenHashSet and OpenHashBigSet implementations. */
  long MAX_OPEN_HASHSET_CAPACITY = 800000000;

  /** Removes all of the elements from this list. */
  void clear();

  /**
   * Number of elements in this list
   *
   * @return size
   */
  long size();

  /**
   * Makes sure set is not using space with capacity more than
   * max(n,size()) entries.
   *
   * @param n the threshold for the trimming.
   */
  void trim(long n);

  /**
   * Adds value to the set.
   * Returns <tt>true</tt> if set changed as a
   * result of the call.
   *
   * @param value Value to add
   * @return true if set was changed.
   */
  boolean add(T value);

  /**
   * Checks whether set contains given value
   *
   * @param value Value to check
   * @return true if value is present in the set
   */
  boolean contains(T value);

  /**
   * TypeOps for type of elements this object holds
   *
   * @return TypeOps
   */
  PrimitiveIdTypeOps<T> getElementTypeOps();

  /** IntWritable implementation of BasicSet */
  public static final class BasicIntOpenHashSet
    implements BasicSet<IntWritable> {
    /** Set */
    private final IntSet set;

    /** Constructor */
    public BasicIntOpenHashSet() {
      set = new IntOpenHashSet();
    }

    /**
     * Constructor
     *
     * @param capacity Capacity
     */
    public BasicIntOpenHashSet(long capacity) {
      if (capacity <= MAX_OPEN_HASHSET_CAPACITY) {
        set = new IntOpenHashSet((int) capacity);
      } else {
        set = new IntOpenHashBigSet(capacity);
      }
    }

    @Override
    public void clear() {
      set.clear();
    }

    @Override
    public long size() {
      if (set instanceof IntOpenHashBigSet) {
        return ((IntOpenHashBigSet) set).size64();
      }
      return set.size();
    }

    @Override
    public void trim(long n) {
      if (set instanceof IntOpenHashSet) {
        ((IntOpenHashSet) set).trim((int) Math.max(set.size(), n));
      } else {
        ((IntOpenHashBigSet) set).trim(Math.max(set.size(), n));
      }
    }

    @Override
    public boolean add(IntWritable value) {
      return set.add(value.get());
    }

    @Override
    public boolean contains(IntWritable value) {
      return set.contains(value.get());
    }

    @Override
    public PrimitiveIdTypeOps<IntWritable> getElementTypeOps() {
      return IntTypeOps.INSTANCE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(set.size());
      IntIterator iter = set.iterator();
      while (iter.hasNext()) {
        out.writeInt(iter.nextInt());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      long size = in.readLong();
      set.clear();
      trim(size);
      for (long i = 0; i < size; ++i) {
        set.add(in.readInt());
      }
    }
  }

  /** LongWritable implementation of BasicSet */
  public static final class BasicLongOpenHashSet
    implements BasicSet<LongWritable> {
    /** Set */
    private final LongSet set;

    /** Constructor */
    public BasicLongOpenHashSet() {
      set = new LongOpenHashSet();
    }

    /**
     * Constructor
     *
     * @param capacity Capacity
     */
    public BasicLongOpenHashSet(long capacity) {
      if (capacity <= MAX_OPEN_HASHSET_CAPACITY) {
        set = new LongOpenHashSet((int) capacity);
      } else {
        set = new LongOpenHashBigSet(capacity);
      }
    }

    @Override
    public void clear() {
      set.clear();
    }

    @Override
    public long size() {
      if (set instanceof LongOpenHashBigSet) {
        return ((LongOpenHashBigSet) set).size64();
      }
      return set.size();
    }

    @Override
    public void trim(long n) {
      if (set instanceof LongOpenHashSet) {
        ((LongOpenHashSet) set).trim((int) Math.max(set.size(), n));
      } else {
        ((LongOpenHashBigSet) set).trim(Math.max(set.size(), n));
      }
    }

    @Override
    public boolean add(LongWritable value) {
      return set.add(value.get());
    }

    @Override
    public boolean contains(LongWritable value) {
      return set.contains(value.get());
    }

    @Override
    public PrimitiveIdTypeOps<LongWritable> getElementTypeOps() {
      return LongTypeOps.INSTANCE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(set.size());
      LongIterator iter = set.iterator();
      while (iter.hasNext()) {
        out.writeLong(iter.nextLong());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      long size = in.readLong();
      set.clear();
      trim(size);
      for (long i = 0; i < size; ++i) {
        set.add(in.readLong());
      }
    }
  }
}

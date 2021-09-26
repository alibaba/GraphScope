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

import org.apache.giraph.types.ops.collections.Basic2ObjectMap.BasicLong2ObjectOpenHashMap;
import org.apache.giraph.types.ops.collections.BasicSet.BasicLongOpenHashSet;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.IOException;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** TypeOps implementation for working with LongWritable type */
public enum LongTypeOps implements
    PrimitiveIdTypeOps<LongWritable>, NumericTypeOps<LongWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<LongWritable> getTypeClass() {
    return LongWritable.class;
  }

  @Override
  public LongWritable create() {
    return new LongWritable();
  }

  @Override
  public LongWritable createCopy(LongWritable from) {
    return new LongWritable(from.get());
  }

  @Override
  public void set(LongWritable to, LongWritable from) {
    to.set(from.get());
  }

  @Override
  public WLongArrayList createArrayList() {
    return new WLongArrayList();
  }

  @Override
  public WLongArrayList createArrayList(int capacity) {
    return new WLongArrayList(capacity);
  }

  @Override
  public WLongArrayList readNewArrayList(DataInput in) throws IOException {
    return WLongArrayList.readNew(in);
  }

  @Override
  public BasicLongOpenHashSet createOpenHashSet() {
    return new BasicLongOpenHashSet();
  }

  @Override
  public BasicLongOpenHashSet createOpenHashSet(long capacity) {
    return new BasicLongOpenHashSet(capacity);
  }

  @Override
  public <V> BasicLong2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      WritableWriter<V> valueWriter) {
    return new BasicLong2ObjectOpenHashMap<>(valueWriter);
  }

  @Override
  public <V> BasicLong2ObjectOpenHashMap<V> create2ObjectOpenHashMap(
      int capacity, WritableWriter<V> valueWriter) {
    return new BasicLong2ObjectOpenHashMap<>(capacity, valueWriter);
  }

  @Override
  public LongWritable createZero() {
    return new LongWritable(0);
  }

  @Override
  public LongWritable createOne() {
    return new LongWritable(1);
  }

  @Override
  public LongWritable createMinNegativeValue() {
    return new LongWritable(Long.MIN_VALUE);
  }

  @Override
  public LongWritable createMaxPositiveValue() {
    return new LongWritable(Long.MAX_VALUE);
  }

  @Override
  public void plusInto(LongWritable value, LongWritable increment) {
    value.set(value.get() + increment.get());
  }

  @Override
  public void multiplyInto(LongWritable value, LongWritable multiplier) {
    value.set(value.get() * multiplier.get());
  }

  @Override
  public void negate(LongWritable value) {
    value.set(-value.get());
  }

  @Override
  public int compare(LongWritable value1, LongWritable value2) {
    return Long.compare(value1.get(), value2.get());
  }
}

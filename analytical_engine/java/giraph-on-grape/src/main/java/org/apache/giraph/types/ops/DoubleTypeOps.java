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

import org.apache.giraph.types.ops.collections.array.WDoubleArrayList;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.IOException;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** TypeOps implementation for working with DoubleWritable type */
public enum DoubleTypeOps implements
    PrimitiveTypeOps<DoubleWritable>, NumericTypeOps<DoubleWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<DoubleWritable> getTypeClass() {
    return DoubleWritable.class;
  }

  @Override
  public DoubleWritable create() {
    return new DoubleWritable();
  }

  @Override
  public DoubleWritable createCopy(DoubleWritable from) {
    return new DoubleWritable(from.get());
  }

  @Override
  public void set(DoubleWritable to, DoubleWritable from) {
    to.set(from.get());
  }

  @Override
  public WDoubleArrayList createArrayList() {
    return new WDoubleArrayList();
  }

  @Override
  public WDoubleArrayList createArrayList(int capacity) {
    return new WDoubleArrayList(capacity);
  }

  @Override
  public WDoubleArrayList readNewArrayList(DataInput in) throws IOException {
    return WDoubleArrayList.readNew(in);
  }

  @Override
  public DoubleWritable createZero() {
    return new DoubleWritable(0);
  }

  @Override
  public DoubleWritable createOne() {
    return new DoubleWritable(1);
  }

  @Override
  public DoubleWritable createMinNegativeValue() {
    return new DoubleWritable(Double.NEGATIVE_INFINITY);
  }

  @Override
  public DoubleWritable createMaxPositiveValue() {
    return new DoubleWritable(Double.POSITIVE_INFINITY);
  }

  @Override
  public void plusInto(DoubleWritable value, DoubleWritable increment) {
    value.set(value.get() + increment.get());
  }

  @Override
  public void multiplyInto(DoubleWritable value, DoubleWritable multiplier) {
    value.set(value.get() * multiplier.get());
  }

  @Override
  public void negate(DoubleWritable value) {
    value.set(-value.get());
  }

  @Override
  public int compare(DoubleWritable value1, DoubleWritable value2) {
    return Double.compare(value1.get(), value2.get());
  }
}

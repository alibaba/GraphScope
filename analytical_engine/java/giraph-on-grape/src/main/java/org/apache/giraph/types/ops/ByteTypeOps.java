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

import org.apache.giraph.types.ops.collections.array.WByteArrayList;
import org.apache.hadoop.io.ByteWritable;

import java.io.DataInput;
import java.io.IOException;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** TypeOps implementation for working with ByteWritable type */
public enum ByteTypeOps implements
    PrimitiveTypeOps<ByteWritable>, NumericTypeOps<ByteWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<ByteWritable> getTypeClass() {
    return ByteWritable.class;
  }

  @Override
  public ByteWritable create() {
    return new ByteWritable();
  }

  @Override
  public ByteWritable createCopy(ByteWritable from) {
    return new ByteWritable(from.get());
  }

  @Override
  public void set(ByteWritable to, ByteWritable from) {
    to.set(from.get());
  }

  @Override
  public WByteArrayList createArrayList() {
    return new WByteArrayList();
  }

  @Override
  public WByteArrayList createArrayList(int capacity) {
    return new WByteArrayList(capacity);
  }

  @Override
  public WByteArrayList readNewArrayList(DataInput in) throws IOException {
    return WByteArrayList.readNew(in);
  }

  @Override
  public ByteWritable createZero() {
    return new ByteWritable((byte) 0);
  }

  @Override
  public ByteWritable createOne() {
    return new ByteWritable((byte) 1);
  }

  @Override
  public ByteWritable createMinNegativeValue() {
    return new ByteWritable(Byte.MIN_VALUE);
  }

  @Override
  public ByteWritable createMaxPositiveValue() {
    return new ByteWritable(Byte.MAX_VALUE);
  }

  @Override
  public void plusInto(ByteWritable value, ByteWritable increment) {
    value.set((byte) (value.get() + increment.get()));
  }

  @Override
  public void multiplyInto(ByteWritable value, ByteWritable multiplier) {
    value.set((byte) (value.get() * multiplier.get()));
  }

  @Override
  public void negate(ByteWritable value) {
    value.set((byte) (-value.get()));
  }

  @Override
  public int compare(ByteWritable value1, ByteWritable value2) {
    return Byte.compare(value1.get(), value2.get());
  }
}

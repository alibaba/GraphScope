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

import org.apache.giraph.types.ops.collections.array.WBooleanArrayList;
import org.apache.hadoop.io.BooleanWritable;

import java.io.DataInput;
import java.io.IOException;

// AUTO-GENERATED class via class:
// org.apache.giraph.generate.GeneratePrimitiveClasses

/** TypeOps implementation for working with BooleanWritable type */
public enum BooleanTypeOps implements
    PrimitiveTypeOps<BooleanWritable> {
  /** Singleton instance */
  INSTANCE;

  @Override
  public Class<BooleanWritable> getTypeClass() {
    return BooleanWritable.class;
  }

  @Override
  public BooleanWritable create() {
    return new BooleanWritable();
  }

  @Override
  public BooleanWritable createCopy(BooleanWritable from) {
    return new BooleanWritable(from.get());
  }

  @Override
  public void set(BooleanWritable to, BooleanWritable from) {
    to.set(from.get());
  }

  @Override
  public WBooleanArrayList createArrayList() {
    return new WBooleanArrayList();
  }

  @Override
  public WBooleanArrayList createArrayList(int capacity) {
    return new WBooleanArrayList(capacity);
  }

  @Override
  public WBooleanArrayList readNewArrayList(DataInput in) throws IOException {
    return WBooleanArrayList.readNew(in);
  }
}

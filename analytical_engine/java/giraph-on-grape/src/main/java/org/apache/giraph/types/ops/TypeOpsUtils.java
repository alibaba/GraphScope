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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

/**
 * Utility functions for getting TypeOps instances from class types.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TypeOpsUtils {
  /** No instances */
  private TypeOpsUtils() { }

  /**
   * Get PrimitiveIdTypeOps for given type, or null if there is none.
   * @param type Class type
   * @param <T> Type
   * @return PrimitiveIdTypeOps
   */
  public static <T>
  PrimitiveIdTypeOps<T> getPrimitiveIdTypeOpsOrNull(Class<T> type) {
    if (type.equals(LongWritable.class)) {
      return (PrimitiveIdTypeOps) LongTypeOps.INSTANCE;
    } else if (type.equals(IntWritable.class)) {
      return (PrimitiveIdTypeOps) IntTypeOps.INSTANCE;
    } else {
      return null;
    }
  }

  /**
   * Get PrimitiveIdTypeOps for given type.
   * Exception will be thrown if there is none.
   * @param type Class type
   * @param <T> Type
   * @return PrimitiveIdTypeOps
   */
  public static <T>
  PrimitiveIdTypeOps<T> getPrimitiveIdTypeOps(Class<T> type) {
    PrimitiveIdTypeOps<T> typeOps = getPrimitiveIdTypeOpsOrNull(type);
    if (typeOps != null) {
      return typeOps;
    } else {
      throw new IllegalArgumentException(
          type + " not supported in PrimitiveIdTypeOps");
    }
  }

  /**
   * Get PrimitiveTypeOps for given type, or null if there is none.
   * @param type Class type
   * @param <T> Type
   * @return PrimitiveTypeOps
   */
  public static <T>
  PrimitiveTypeOps<T> getPrimitiveTypeOpsOrNull(Class<T> type) {
    PrimitiveTypeOps<T> typeOps = getPrimitiveIdTypeOpsOrNull(type);
    if (typeOps != null) {
      return typeOps;
    } else if (type.equals(FloatWritable.class)) {
      return (PrimitiveTypeOps) FloatTypeOps.INSTANCE;
    } else if (type.equals(DoubleWritable.class)) {
      return (PrimitiveTypeOps) DoubleTypeOps.INSTANCE;
    } else if (type.equals(BooleanWritable.class)) {
      return (PrimitiveTypeOps) BooleanTypeOps.INSTANCE;
    } else if (type.equals(ByteWritable.class)) {
      return (PrimitiveTypeOps) ByteTypeOps.INSTANCE;
    } else {
      return null;
    }
  }

  /**
   * Get PrimitiveTypeOps for given type.
   * Exception will be thrown if there is none.
   * @param type Class type
   * @param <T> Type
   * @return PrimitiveTypeOps
   */
  public static <T>
  PrimitiveTypeOps<T> getPrimitiveTypeOps(Class<T> type) {
    PrimitiveTypeOps<T> typeOps = getPrimitiveTypeOpsOrNull(type);
    if (typeOps != null) {
      return typeOps;
    } else {
      throw new IllegalArgumentException(
          type + " not supported in PrimitiveTypeOps");
    }
  }

  /**
   * Get TypeOps for given type, or null if there is none.
   * @param type Class type
   * @param <T> Type
   * @return TypeOps
   */
  public static <T> TypeOps<T> getTypeOpsOrNull(Class<T> type) {
    TypeOps<T> typeOps = getPrimitiveTypeOpsOrNull(type);
    if (typeOps != null) {
      return typeOps;
    } else if (type.equals(Text.class)) {
      return (TypeOps) TextTypeOps.INSTANCE;
    } else if (type.equals(MapWritable.class)) {
      return (TypeOps) MapTypeOps.INSTANCE;
    } else {
      return null;
    }
  }

  /**
   * Get TypeOps for given type.
   * Exception will be thrown if there is none.
   * @param type Class type
   * @param <T> Type
   * @return TypeOps
   */
  public static <T> TypeOps<T> getTypeOps(Class<T> type) {
    TypeOps<T> typeOps = getTypeOpsOrNull(type);
    if (typeOps != null) {
      return typeOps;
    } else {
      throw new IllegalArgumentException(
          type + " not supported in TypeOps");
    }
  }

  /**
   * Write TypeOps object into a stream
   * @param typeOps type ops instance
   * @param output output stream
   * @param <T> Corresponding type
   */
  public static <T> void writeTypeOps(TypeOps<T> typeOps,
      DataOutput output) throws IOException {
    WritableUtils.writeEnum((Enum) typeOps, output);
  }

  /**
   * Read TypeOps object from the stream
   * @param input input stream
   * @param <O> Concrete TypeOps type
   * @return type ops instance
   */
  public static <O extends TypeOps<?>> O readTypeOps(
      DataInput input) throws IOException {
    return  (O) WritableUtils.readEnum(input);
  }
}

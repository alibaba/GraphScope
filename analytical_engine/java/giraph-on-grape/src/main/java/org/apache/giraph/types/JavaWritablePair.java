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
package org.apache.giraph.types;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * Holder for java and writable class pair.
 *
 * @param <W> writable class type
 * @param <J> java class type
 */
public class JavaWritablePair<W extends Writable, J> {
  /** Boolean,BooleanWritable */
  public static final JavaWritablePair<BooleanWritable, Boolean>
  BOOLEAN_BOOLEAN_WRITABLE = create(BooleanWritable.class, Boolean.class);
  /** Byte,ByteWritable */
  public static final JavaWritablePair<ByteWritable, Byte>
  BYTE_BYTE_WRITABLE = create(ByteWritable.class, Byte.class);
  /** Byte,IntWritable */
  public static final JavaWritablePair<IntWritable, Byte>
  BYTE_INT_WRITABLE = create(IntWritable.class, Byte.class);
  /** Byte,LongWritable */
  public static final JavaWritablePair<LongWritable, Byte>
  BYTE_LONG_WRITABLE = create(LongWritable.class, Byte.class);
  /** Double,FloatWritable */
  public static final JavaWritablePair<FloatWritable, Double>
  DOUBLE_FLOAT_WRITABLE = create(FloatWritable.class, Double.class);
  /** Double,DoubleWritable */
  public static final JavaWritablePair<DoubleWritable, Double>
  DOUBLE_DOUBLE_WRITABLE = create(DoubleWritable.class, Double.class);
  /** Float,FloatWritable */
  public static final JavaWritablePair<FloatWritable, Float>
  FLOAT_FLOAT_WRITABLE = create(FloatWritable.class, Float.class);
  /** Float,DoubleWritable */
  public static final JavaWritablePair<DoubleWritable, Float>
  FLOAT_DOUBLE_WRITABLE = create(DoubleWritable.class, Float.class);
  /** Integer,ByteWritable */
  public static final JavaWritablePair<ByteWritable, Integer>
  INT_BYTE_WRITABLE = create(ByteWritable.class, Integer.class);
  /** Integer,IntWritable */
  public static final JavaWritablePair<IntWritable, Integer>
  INT_INT_WRITABLE = create(IntWritable.class, Integer.class);
  /** Integer,LongWritable */
  public static final JavaWritablePair<LongWritable, Integer>
  INT_LONG_WRITABLE = create(LongWritable.class, Integer.class);
  /** Long,ByteWritable */
  public static final JavaWritablePair<ByteWritable, Long>
  LONG_BYTE_WRITABLE = create(ByteWritable.class, Long.class);
  /** Long,IntWritable */
  public static final JavaWritablePair<IntWritable, Long>
  LONG_INT_WRITABLE = create(IntWritable.class, Long.class);
  /** Long,LongWritable */
  public static final JavaWritablePair<LongWritable, Long>
  LONG_LONG_WRITABLE = create(LongWritable.class, Long.class);
  /** Short,ByteWritable */
  public static final JavaWritablePair<ByteWritable, Short>
  SHORT_BYTE_WRITABLE = create(ByteWritable.class, Short.class);
  /** Short,IntWritable */
  public static final JavaWritablePair<IntWritable, Short>
  SHORT_INT_WRITABLE = create(IntWritable.class, Short.class);
  /** Short,LongWritable */
  public static final JavaWritablePair<LongWritable, Short>
  SHORT_LONG_WRITABLE = create(LongWritable.class, Short.class);

  /** java class */
  private final Class<J> javaClass;
  /** writable class */
  private final Class<W> writableClass;

  /**
   * Constructor
   *
   * @param javaClass java class
   * @param writableClass writable class
   */
  private JavaWritablePair(Class<W> writableClass, Class<J> javaClass) {
    this.javaClass = javaClass;
    this.writableClass = writableClass;
  }

  /**
   * Create holder for classes
   *
   * @param writableClass writable class
   * @param javaClass java class
   * @param <W> writable class type
   * @param <J> java class type
   * @return JavaAndWritableClasses
   */
  public static <W extends Writable, J> JavaWritablePair<W, J> create(
      Class<W> writableClass, Class<J> javaClass) {
    return new JavaWritablePair<W, J>(writableClass, javaClass);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof JavaWritablePair) {
      JavaWritablePair other = (JavaWritablePair) obj;
      return Objects.equal(javaClass, other.javaClass) &&
          Objects.equal(writableClass, other.writableClass);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(javaClass, writableClass);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("javaClass", javaClass.getSimpleName())
        .add("writableClass", writableClass.getSimpleName())
        .toString();
  }

  public Class<J> getJavaClass() {
    return javaClass;
  }

  public Class<W> getWritableClass() {
    return writableClass;
  }
}

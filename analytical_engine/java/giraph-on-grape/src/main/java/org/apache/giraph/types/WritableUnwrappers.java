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

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.giraph.types.JavaWritablePair.BOOLEAN_BOOLEAN_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.BYTE_BYTE_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.DOUBLE_DOUBLE_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.DOUBLE_FLOAT_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.FLOAT_FLOAT_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.INT_BYTE_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.INT_INT_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.LONG_BYTE_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.LONG_INT_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.LONG_LONG_WRITABLE;
import static org.apache.giraph.types.JavaWritablePair.SHORT_BYTE_WRITABLE;

/**
 * Mapping of all the known Writable wrappers.
 */
public class WritableUnwrappers {
  /** Map of (Writable,Java)-type pair to wrapper for those types */
  private static final Map<JavaWritablePair, WritableUnwrapper> MAP;

  static {
    MAP = Maps.newHashMap();
    MAP.put(BOOLEAN_BOOLEAN_WRITABLE, new BooleanWritableToBooleanUnwrapper());
    MAP.put(BYTE_BYTE_WRITABLE, new ByteWritableToByteUnwrapper());
    MAP.put(DOUBLE_DOUBLE_WRITABLE, new DoubleWritableToDoubleUnwrapper());
    MAP.put(DOUBLE_FLOAT_WRITABLE, new FloatWritableToDoubleUnwrapper());
    MAP.put(FLOAT_FLOAT_WRITABLE, new FloatWritableToFloatUnwrapper());
    MAP.put(INT_BYTE_WRITABLE, new ByteWritableToIntUnwrapper());
    MAP.put(INT_INT_WRITABLE, new IntWritableToIntUnwrapper());
    MAP.put(SHORT_BYTE_WRITABLE, new ByteWritableToShortUnwrapper());
    MAP.put(LONG_BYTE_WRITABLE, new ByteWritableToLongUnwrapper());
    MAP.put(LONG_INT_WRITABLE, new IntWritableToLongUnwrapper());
    MAP.put(LONG_LONG_WRITABLE, new LongWritableToLongUnwrapper());
  }

  /** Don't construct */
  private WritableUnwrappers() { }

  /**
   * Lookup type converter
   *
   * @param writableClass class of Writable
   * @param javaClass class of Java type
   * @param <W> Writable type
   * @param <J> Java type
   * @return {@link WritableUnwrapper}
   */
  public static <W extends Writable, J> WritableUnwrapper<W, J> lookup(
      Class<W> writableClass, Class<J> javaClass) {
    return lookup(JavaWritablePair.create(writableClass, javaClass));
  }

  /**
   * Lookup type converter
   *
   * @param classes JavaAndWritableClasses
   * @param <W> Writable type
   * @param <J> Java type
   * @return {@link WritableUnwrapper}
   */
  public static <W extends Writable, J> WritableUnwrapper<W, J> lookup(
      JavaWritablePair<W, J> classes) {
    return MAP.get(classes);
  }
}

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
package org.apache.giraph.writable.kryo.serializers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A custom Serializer that will call the Writable methods defined by the
 * object itself to serialize the object, instead of Kryo auto-magically
 * serializing
 *
 * @param <T> Object type, should implement Writable
 */

public class DirectWritableSerializer<T extends Writable>
    extends Serializer<T> {

  @Override
  public void write(Kryo kryo, Output output, T object) {
    try {
      object.write(new DataOutputStream(output));
    } catch (IOException e) {
      throw new RuntimeException(
          "DirectWritableSerializer.write calling Writable method of class: " +
            object.getClass().getName() + " encountered issues", e);
    }
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type) {
    try {
      T object = create(kryo, input, type);
      kryo.reference(object);
      object.readFields(new DataInputStream(input));

      return object;
    } catch (IOException e) {
      throw new RuntimeException(
          "DirectWritableSerializer.read calling Writable method of class: " +
          type.getName() + " encountered issues", e);
    }
  }

  @Override
  public T copy(Kryo kryo, T original) {
    return WritableUtils.createCopy(original);
  }

  /**
   * Used by {@link #read(Kryo, Input, Class)} to create the new object.
   * This can be overridden to customize object creation, eg to call a
   * constructor with arguments. The default implementation
   * uses {@link Kryo#newInstance(Class)}.
   *
   * @param kryo Kryo object instance
   * @param input Input
   * @param type Type of the class to create
   * @return New instance of wanted type
   */
  protected T create(Kryo kryo, Input input, Class<T> type) {
    return ReflectionUtils.newInstance(type);
  }
}

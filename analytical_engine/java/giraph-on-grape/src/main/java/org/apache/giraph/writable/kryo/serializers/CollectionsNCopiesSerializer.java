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

import java.util.Collections;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Special serializer for Collections.nCopies
 *
 * @param <T> Element type
 */
public class CollectionsNCopiesSerializer<T> extends Serializer<List<T>> {
  @Override
  public void write(Kryo kryo, Output output, List<T> object) {
    output.writeInt(object.size(), true);
    if (object.size() > 0) {
      kryo.writeClassAndObject(output, object.get(0));
    }
  }

  @Override
  public List<T> read(Kryo kryo, Input input, Class<List<T>> type) {
    int size = input.readInt(true);
    if (size > 0) {
      T object = (T) kryo.readClassAndObject(input);
      return Collections.nCopies(size, object);
    } else {
      return Collections.emptyList();
    }
  }
}

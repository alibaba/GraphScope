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


import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.ImmutableBiMap;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;

/**
 * Helper class used to register serializers for the different classes
 * return by the {@link ImmutableBiMap} methods.
 */
public class ImmutableBiMapSerializerUtils {

  /**
   * Private default constructor
   */
  private ImmutableBiMapSerializerUtils() {
  }

  /**
   * Registers different {@link ImmutableBiMap} implementations with Kryo.
   * @param kryo {@link Kryo} instance to register class with.
   */
  public static void registerSerializers(Kryo kryo) {
    ImmutableMapSerializer serializer = new ImmutableMapSerializer();
    kryo.register(ImmutableBiMap.class, serializer);
    kryo.register(ImmutableBiMap.of().getClass(), serializer);
    Object o1 = new Object();
    Object o2 = new Object();
    Object o3 = new Object();
    Object o4 = new Object();
    Object o5 = new Object();
    kryo.register(ImmutableBiMap.of(o1, o1).getClass(), serializer);
    kryo.register(ImmutableBiMap.of(o1, o1, o2, o2).getClass(), serializer);
    kryo.register(ImmutableBiMap.of(o1, o1, o2, o2, o3, o3)
      .getClass(), serializer);
    kryo.register(ImmutableBiMap.of(o1, o1, o2, o2, o3, o3, o4, o4)
      .getClass(), serializer);
    kryo.register(ImmutableBiMap.of(o1, o1, o2, o2, o3, o3, o4, o4, o5, o5)
      .getClass(), serializer);
  }
}

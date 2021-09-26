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
package org.apache.giraph.types.ops.collections;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Handler for knowing how to serialize/deserialize type T
 *
 * @param <T> Type of object to be serialized.
 */
public interface WritableWriter<T> {
  /**
   * Serialize the fields of <code>value</code> to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize object into.
   * @param value Object to serialize
   * @throws IOException
   */
  void write(DataOutput out, T value) throws IOException;

  /**
   * Deserialize the fields of object from <code>in</code>.
   *
   * @param in <code>DataInput</code> to deseriablize object from.
   * @return Deserialized object.
   * @throws IOException
   */
  T readFields(DataInput in) throws IOException;
}

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

import org.apache.hadoop.io.MapWritable;

/** TypeOps implementation for working with MapWritable type */
public enum MapTypeOps implements TypeOps<MapWritable> {
  /** Singleton instance */
  INSTANCE();

  @Override
  public Class<MapWritable> getTypeClass() {
    return MapWritable.class;
  }

  @Override
  public MapWritable create() {
    return new MapWritable();
  }

  @Override
  public MapWritable createCopy(MapWritable from) {
    return new MapWritable(from);
  }

  @Override
  public void set(MapWritable to, MapWritable from) {
    to.clear();
    to.putAll(from);
  }
}

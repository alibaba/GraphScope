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

package org.apache.giraph.conf;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default implementation of ImmutableClassesGiraphConfigurable
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DefaultImmutableClassesGiraphConfigurable<
    I extends WritableComparable, V extends Writable,
    E extends Writable> implements
    ImmutableClassesGiraphConfigurable<I, V, E> {
    /** Configuration */
    private ImmutableClassesGiraphConfiguration<I, V, E> conf;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        this.conf = conf;
    }

    @Override
    public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
        return conf;
    }
}

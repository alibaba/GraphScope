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

package org.apache.giraph.io;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Use this to load data for a BSP application.  Note that the InputSplit must
 * also implement Writable.  The InputSplits will determine the partitioning of
 * vertices across the mappers, so keep that in consideration when implementing
 * getSplits().  Provides access to ImmutableClassesGiraphConfiguration.
 *
 * It's guaranteed that whatever parameters are set in the configuration are
 * also going to be available in all method arguments related to this input
 * format (context in getSplits and createVertexReader; methods invoked on
 * VertexReader). So if backing input format relies on some parameters from
 * configuration, you can safely set them for example in
 * {@link #setConf(org.apache.giraph.conf.ImmutableClassesGiraphConfiguration)}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class VertexInputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable> extends GiraphInputFormat<I, V, E> {
    /**
     * Create a vertex reader for a given split. Guaranteed to have been
     * configured with setConf() prior to use.  The framework will also call
     * {@link VertexReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     *
     * @param split the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     */
    public abstract VertexReader<I, V, E> createVertexReader(
        InputSplit split,
        TaskAttemptContext context) throws IOException;
}

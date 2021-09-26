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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Input format for reading single edges.  Provides access to ImmutableClassesGiraphConfiguration.
 * <p>
 * It's guaranteed that whatever parameters are set in the configuration are also going to be
 * available in all method arguments related to this input format (context in getSplits and
 * createEdgeReader; methods invoked on EdgeReader). So if backing input format relies on some
 * parameters from configuration, you can safely set them for example in {@link
 * #setConf(org.apache.giraph.conf.ImmutableClassesGiraphConfiguration)}.
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public abstract class EdgeInputFormat<I extends WritableComparable, E extends Writable>
        extends GiraphInputFormat<I, Writable, E> {

    /**
     * Create an edge reader for a given split. The framework will call {@link
     * EdgeReader#initialize(InputSplit, TaskAttemptContext)} before the split is used.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     */
    public abstract EdgeReader<I, E> createEdgeReader(InputSplit split, TaskAttemptContext context)
            throws IOException;
}

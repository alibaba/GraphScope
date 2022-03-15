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
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Implement to output the graph after the computation.  It is modeled directly after the Hadoop
 * OutputFormat. ImmutableClassesGiraphConfiguration is available
 * <p>
 * It's guaranteed that whatever parameters are set in the configuration are also going to be
 * available in all method arguments related to this output format (context in createVertexWriter,
 * checkOutputSpecs and getOutputCommitter; methods invoked on VertexWriter and OutputCommitter). So
 * if backing output format relies on some parameters from configuration, you can safely set them
 * for example in {@link #setConf(org.apache.giraph.conf.ImmutableClassesGiraphConfiguration)}.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class VertexOutputFormat<
                I extends WritableComparable, V extends Writable, E extends Writable>
        extends OutputFormat<I, V, E> {

    /**
     * Create a vertex writer for a given split. The framework will call {@link
     * VertexWriter#initialize(TaskAttemptContext)} before the split is used.
     *
     * @param context the information about the task
     * @return a new vertex writer
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException;
}

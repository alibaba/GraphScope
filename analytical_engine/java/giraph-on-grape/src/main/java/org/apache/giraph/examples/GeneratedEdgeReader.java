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

package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Used by GeneratedEdgeInputFormat
 * to read some generated data
 *
 * @param <I> Vertex index value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class GeneratedEdgeReader<
    I extends WritableComparable,
    E extends Writable>
    extends EdgeReader<I, E> {
  /** Default edges produced by this reader */
  public static final LongConfOption DEFAULT_READER_EDGES =
    new LongConfOption("GeneratedEdgeReader.reader_edges", 10,
        "Default edges produced by this reader");
  /** Records read so far */
  protected long recordsRead = 0;
  /** Total records to read (on this split alone) */
  protected long totalRecords = 0;
  /** The input split from initialize(). */
  protected BspInputSplit inputSplit = null;

  /**
   * Default constructor for reflection.
   */
  public GeneratedEdgeReader() {
  }

  @Override
  public final void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException {
    totalRecords = DEFAULT_READER_EDGES.get(getConf());
    this.inputSplit = (BspInputSplit) inputSplit;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public final float getProgress() throws IOException {
    return recordsRead * 100.0f / totalRecords;
  }
}

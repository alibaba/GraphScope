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
import java.util.Map.Entry;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This is a simple example for an aggregator writer. After each superstep
 * the writer will persist the aggregator values to disk, by use of the
 * Writable interface. The file will be created on the current working
 * directory.
 */
public class SimpleAggregatorWriter extends
    DefaultImmutableClassesGiraphConfigurable implements
    AggregatorWriter {
  /** Name of the file we wrote to */
  private static String FILENAME;
  /** Saved output stream to write to */
  private FSDataOutputStream output;

  public static String getFilename() {
    return FILENAME;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void initialize(Context context, long applicationAttempt)
    throws IOException {
    setFilename(applicationAttempt);
    Path p = new Path(FILENAME);
    FileSystem fs = FileSystem.get(context.getConfiguration());
    output = fs.create(p, true);
  }

  /**
   * Set filename written to
   *
   * @param applicationAttempt app attempt
   */
  private static void setFilename(long applicationAttempt) {
    FILENAME = "aggregatedValues_" + applicationAttempt;
  }

  @Override
  public void writeAggregator(
      Iterable<Entry<String, Writable>> aggregatorMap,
      long superstep) throws IOException {
    for (Entry<String, Writable> entry : aggregatorMap) {
      entry.getValue().write(output);
    }
    output.flush();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }
}

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

import com.google.common.collect.Lists;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * weighted graphs with long ids. Each line consists of: vertex neighbor1:weight
 * neighbor2:weight ...
 */
public class LongDoubleFloatTextInputFormat
    extends TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable>
    implements ImmutableClassesGiraphConfigurable<LongWritable, DoubleWritable,
    FloatWritable> {
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable,
      FloatWritable> conf;

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new LongDoubleFloatVertexReader();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration<LongWritable,
      DoubleWritable, FloatWritable> configuration) {
    this.conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable,
      FloatWritable> getConf() {
    return conf;
  }

  /**
   * Vertex reader associated with
   * {@link LongDoubleDoubleTextInputFormat}.
   */
  public class LongDoubleFloatVertexReader extends
      TextVertexInputFormat<LongWritable, DoubleWritable,
          FloatWritable>.TextVertexReader {
    /** Separator of the vertex and neighbors */
    private final Pattern neighborSeparator = Pattern.compile("[\t ]");
    /** Separator of a neighbor and its weight */
    private final Pattern weightSeparator = Pattern.compile("[:]");

    @Override
    public Vertex<LongWritable, DoubleWritable, FloatWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, DoubleWritable, FloatWritable>
          vertex = conf.createVertex();

      String[] tokens = neighborSeparator.split(getRecordReader()
          .getCurrentValue().toString());
      List<Edge<LongWritable, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);

      for (int n = 1; n < tokens.length; n++) {
        String[] parts = weightSeparator.split(tokens[n]);
        edges.add(EdgeFactory.create(
            new LongWritable(Long.parseLong(parts[0])),
            new FloatWritable(Float.parseFloat(parts[1]))));
      }

      LongWritable vertexId = new LongWritable(Long.parseLong(tokens[0]));
      vertex.initialize(vertexId, new DoubleWritable(), edges);

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}

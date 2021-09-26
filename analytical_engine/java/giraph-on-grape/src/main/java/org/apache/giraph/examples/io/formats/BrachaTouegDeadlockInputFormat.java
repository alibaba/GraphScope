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
package org.apache.giraph.examples.io.formats;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.utils.BrachaTouegDeadlockVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

/**
  * VertexInputFormat for the Bracha Toueg Deadlock Detection algorithm
  * specified in JSON format.
  */
public class BrachaTouegDeadlockInputFormat extends
  TextVertexInputFormat<LongWritable, BrachaTouegDeadlockVertexValue,
    LongWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonLongLongLongLongVertexReader();
  }

 /**
  * VertexReader use for the Bracha Toueg Deadlock Detection Algorithm.
  * The files should be in the following JSON format:
  * JSONArray(<vertex id>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge tag>), ...))
  * The tag is use for the N-out-of-M semantics. Two edges with the same tag
  * are considered to be combined and, hence, represent to combined requests
  * that need to be both satisfied to continue execution.
  * Here is an example with vertex id 1, and three edges (requests).
  * First edge has a destination vertex 2 with tag 0.
  * Second and third edge have a destination vertex respectively of 3 and 4
  * with tag 1.
  * [1,[[2,0], [3,1], [4,1]]]
  */
  class JsonLongLongLongLongVertexReader
    extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
            JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected BrachaTouegDeadlockVertexValue getValue(JSONArray jsonVertex)
      throws JSONException, IOException {

      return new BrachaTouegDeadlockVertexValue();
    }

    @Override
    protected Iterable<Edge<LongWritable, LongWritable>>
    getEdges(JSONArray jsonVertex) throws JSONException, IOException {

      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);

      /* get the edges */
      List<Edge<LongWritable, LongWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());

      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        LongWritable targetId;
        LongWritable tag;
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);

        targetId = new LongWritable(jsonEdge.getLong(0));
        tag = new LongWritable((long) jsonEdge.getLong(1));
        edges.add(EdgeFactory.create(targetId, tag));
      }
      return edges;
    }

    @Override
    protected Vertex<LongWritable, BrachaTouegDeadlockVertexValue,
      LongWritable> handleException(Text line, JSONArray jsonVertex,
          JSONException e) {

      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }
  }
}

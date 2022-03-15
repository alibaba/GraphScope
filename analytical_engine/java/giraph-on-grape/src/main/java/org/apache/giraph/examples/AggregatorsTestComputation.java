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

/**
 * Computation which uses aggrergators. To be used for testing.
 */
public class AggregatorsTestComputation {}

// extends
//    BasicComputation<LongWritable, DoubleWritable, FloatWritable,
//        DoubleWritable> {

//  /** Name of regular aggregator */
//  private static final String REGULAR_AGG = "regular";
//  /** Name of persistent aggregator */
//  private static final String PERSISTENT_AGG = "persistent";
//  /** Name of input super step persistent aggregator */
//  private static final String INPUT_VERTEX_PERSISTENT_AGG
//    = "input_super_step_vertex_agg";
//  /** Name of input super step persistent aggregator */
//  private static final String INPUT_EDGE_PERSISTENT_AGG
//    = "input_super_step_edge_agg";
//  /** Name of master overwriting aggregator */
//  private static final String MASTER_WRITE_AGG = "master";
//  /** Value which master compute will use */
//  private static final long MASTER_VALUE = 12345;
//  /** Prefix for name of aggregators in array */
//  private static final String ARRAY_PREFIX_AGG = "array";
//  /** Number of aggregators to use in array */
//  private static final int NUM_OF_AGGREGATORS_IN_ARRAY = 100;
//
//  @Override
//  public void compute(
//      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
//      Iterable<DoubleWritable> messages) throws IOException {
//    long superstep = getSuperstep();
//
//    LongWritable myValue = new LongWritable(1L << superstep);
//    aggregate(REGULAR_AGG, myValue);
//    aggregate(PERSISTENT_AGG, myValue);
//
//    long nv = getTotalNumVertices();
//    if (superstep > 0) {
//      assertEquals(nv * (1L << (superstep - 1)),
//          ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
//    } else {
//      assertEquals(0,
//          ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
//    }
//    assertEquals(nv * ((1L << superstep) - 1),
//        ((LongWritable) getAggregatedValue(PERSISTENT_AGG)).get());
//    assertEquals(MASTER_VALUE * (1L << superstep),
//        ((LongWritable) getAggregatedValue(MASTER_WRITE_AGG)).get());
//
//    for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
//      aggregate(ARRAY_PREFIX_AGG + i, new LongWritable((superstep + 1) * i));
//      assertEquals(superstep * getTotalNumVertices() * i,
//          ((LongWritable) getAggregatedValue(ARRAY_PREFIX_AGG + i)).get());
//    }
//
//    if (getSuperstep() == 10) {
//      vertex.voteToHalt();
//    }
//  }
//
//  /** Master compute which uses aggregators. To be used for testing. */
//  public static class AggregatorsTestMasterCompute extends
//      DefaultMasterCompute {
//    @Override
//    public void compute() {
//      long superstep = getSuperstep();
//
//      LongWritable myValue =
//          new LongWritable(MASTER_VALUE * (1L << superstep));
//      setAggregatedValue(MASTER_WRITE_AGG, myValue);
//
//      long nv = getTotalNumVertices();
//      if (superstep >= 0) {
//        assertEquals(100, ((LongWritable)
//          getAggregatedValue(INPUT_VERTEX_PERSISTENT_AGG)).get());
//      }
//      if (superstep >= 0) {
//        assertEquals(4500, ((LongWritable)
//          getAggregatedValue(INPUT_EDGE_PERSISTENT_AGG)).get());
//      }
//      if (superstep > 0) {
//        assertEquals(nv * (1L << (superstep - 1)),
//            ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
//      } else {
//        assertEquals(0,
//            ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
//      }
//      assertEquals(nv * ((1L << superstep) - 1),
//          ((LongWritable) getAggregatedValue(PERSISTENT_AGG)).get());
//
//      for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
//        assertEquals(superstep * getTotalNumVertices() * i,
//            ((LongWritable) getAggregatedValue(ARRAY_PREFIX_AGG + i)).get());
//      }
//    }
//
//    @Override
//    public void initialize() throws InstantiationException,
//        IllegalAccessException {
//      registerPersistentAggregator(
//          INPUT_VERTEX_PERSISTENT_AGG, LongSumAggregator.class);
//      registerPersistentAggregator(
//          INPUT_EDGE_PERSISTENT_AGG, LongSumAggregator.class);
//      registerAggregator(REGULAR_AGG, LongSumAggregator.class);
//      registerPersistentAggregator(PERSISTENT_AGG,
//          LongSumAggregator.class);
//      registerAggregator(MASTER_WRITE_AGG, LongSumAggregator.class);
//
//      for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
//        registerAggregator(ARRAY_PREFIX_AGG + i, LongSumAggregator.class);
//      }
//    }
//  }
//
//  /**
//   * Throws exception if values are not equal.
//   *
//   * @param expected Expected value
//   * @param actual   Actual value
//   */
//  private static void assertEquals(long expected, long actual) {
//    if (expected != actual) {
//      throw new RuntimeException("expected: " + expected +
//          ", actual: " + actual);
//    }
//  }
//
//  /**
//   * Simple VertexReader
//   */
//  public static class SimpleVertexReader extends
//      GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable> {
//    /** Class logger */
//    private static final Logger LOG =
//        LoggerFactory.getLogger(SimpleVertexReader.class);
//
//    @Override
//    public boolean nextVertex() {
//      return totalRecords > recordsRead;
//    }
//
//    @Override
//    public Vertex<LongWritable, DoubleWritable,
//        FloatWritable> getCurrentVertex() throws IOException {
//      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex =
//          getConf().createVertex();
//      LongWritable vertexId = new LongWritable(
//          (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
//      DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
//      long targetVertexId =
//          (vertexId.get() + 1) %
//          (inputSplit.getNumSplits() * totalRecords);
//      float edgeValue = vertexId.get() * 100f;
//      List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
//      edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
//          new FloatWritable(edgeValue)));
//      vertex.initialize(vertexId, vertexValue, edges);
//      ++recordsRead;
//      if (LOG.isInfoEnabled()) {
//        LOG.info("next vertex: Return vertexId=" + vertex.getId().get() +
//            ", vertexValue=" + vertex.getValue() +
//            ", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
//      }
//      aggregate(INPUT_VERTEX_PERSISTENT_AGG,
//        new LongWritable((long) vertex.getValue().get()));
//      return vertex;
//    }
//  }
//
//  /**
//   * Simple VertexInputFormat
//   */
//  public static class SimpleVertexInputFormat extends
//    GeneratedVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
//    @Override
//    public VertexReader<LongWritable, DoubleWritable,
//    FloatWritable> createVertexReader(InputSplit split,
//      TaskAttemptContext context)
//      throws IOException {
//      return new SimpleVertexReader();
//    }
//  }
//
//  /**
//   * Simple Edge Reader
//   */
//  public static class SimpleEdgeReader extends
//    GeneratedEdgeReader<LongWritable, FloatWritable> {
//    /** Class logger */
//    private static final Logger LOG = LoggerFactory.getLogger(SimpleEdgeReader.class);
//
//    @Override
//    public boolean nextEdge() {
//      return totalRecords > recordsRead;
//    }
//
//    @Override
//    public Edge<LongWritable, FloatWritable> getCurrentEdge()
//      throws IOException {
//      LongWritable vertexId = new LongWritable(
//        (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
//      long targetVertexId = (vertexId.get() + 1) %
//        (inputSplit.getNumSplits() * totalRecords);
//      float edgeValue = vertexId.get() * 100f;
//      Edge<LongWritable, FloatWritable> edge = EdgeFactory.create(
//        new LongWritable(targetVertexId), new FloatWritable(edgeValue));
//      ++recordsRead;
//      if (LOG.isInfoEnabled()) {
//        LOG.info("next edge: Return targetVertexId=" + targetVertexId +
//          ", edgeValue=" + edgeValue);
//      }
//      aggregate(INPUT_EDGE_PERSISTENT_AGG, new LongWritable((long) edge
//        .getValue().get()));
//      return edge;
//    }
//
//    @Override
//    public LongWritable getCurrentSourceId() throws IOException,
//      InterruptedException {
//      LongWritable vertexId = new LongWritable(
//        (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
//      return vertexId;
//    }
//  }
//
//  /**
//   * Simple VertexInputFormat
//   */
//  public static class SimpleEdgeInputFormat extends
//    EdgeInputFormat<LongWritable, FloatWritable> {
//    @Override public void checkInputSpecs(Configuration conf) { }
//
//    @Override
//    public EdgeReader<LongWritable, FloatWritable> createEdgeReader(
//      InputSplit split, TaskAttemptContext context) throws IOException {
//      return new SimpleEdgeReader();
//    }
//
//    @Override
//    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
//      throws IOException, InterruptedException {
//      List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
//      for (int i = 0; i < minSplitCountHint; ++i) {
//        inputSplitList.add(new BspInputSplit(i, minSplitCountHint));
//      }
//      return inputSplitList;
//    }
//  }
// }

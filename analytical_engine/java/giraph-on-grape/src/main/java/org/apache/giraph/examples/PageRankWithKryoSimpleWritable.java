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
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.writable.kryo.KryoSimpleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.giraph.examples.PageRankWithKryoSimpleWritable.VertexValue;
import org.apache.giraph.examples.PageRankWithKryoSimpleWritable.MessageValue;
import org.apache.giraph.examples.PageRankWithKryoSimpleWritable.EdgeValue;
import org.slf4j.LoggerFactory;

/**
 * Copy of SimplePageRank, modified to test vertex/edge and
 * message values that derives from KryoSimpleWritable.
 */
@Algorithm(
        name = "Page rank"
)
public class PageRankWithKryoSimpleWritable extends
        BasicComputation<LongWritable, VertexValue,
        EdgeValue, MessageValue> {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Number of supersteps for this static  3;
  /** Logger */
  private static final Logger LOG =
          LoggerFactory.getLogger(PageRankWithKryoSimpleWritable.class);
  /** Sum aggregator name */
  private static String SUM_AGG = "sum";
  /** Min aggregator name */
  private static String MIN_AGG = "min";
  /** Max aggregator name */
  private static String MAX_AGG = "max";

  @Override
  public void compute(
          Vertex<LongWritable, VertexValue,
          EdgeValue> vertex,
          Iterable<MessageValue> messages) throws IOException {
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (MessageValue message : messages) {
        sum += message.get();
      }
      Double value = (0.15f / getTotalNumVertices()) + 0.85f * sum;
      VertexValue vertexValue = new VertexValue(value);
      vertex.setValue(vertexValue);
      aggregate(MAX_AGG, vertexValue);
      aggregate(MIN_AGG, vertexValue);
      aggregate(SUM_AGG, new LongWritable(1));
      LOG.info(vertex.getId() + ": PageRank=" + vertexValue +
              " max=" + getAggregatedValue(MAX_AGG) +
              " min=" + getAggregatedValue(MIN_AGG));
    }

    if (getSuperstep() < MAX_SUPERSTEPS) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex,
          new MessageValue(vertex.getValue().get() / edges));
    } else {
      vertex.voteToHalt();
    }
  }

  /**
   * Worker context used with {@link PageRankWithKryoSimpleWritable}.
   */
  public static class PageRankWithKryoWorkerContext extends
          WorkerContext {
    /** Final max value for verification for local jobs */
    private static double FINAL_MAX;
    /** Final min value for verification for local jobs */
    private static double FINAL_MIN;
    /** Final sum value for verification for local jobs */
    private static long FINAL_SUM;

    public static double getFinalMax() {
      return FINAL_MAX;
    }

    public static double getFinalMin() {
      return FINAL_MIN;
    }

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
            throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
      FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
      FINAL_MAX = this.<VertexValue>getAggregatedValue(MAX_AGG).get();
      FINAL_MIN = this.<VertexValue>getAggregatedValue(MIN_AGG).get();

      LOG.info("aggregatedNumVertices=" + FINAL_SUM);
      LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
      LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
    }

    @Override
    public void preSuperstep() {
      if (getSuperstep() >= 3) {
        LOG.info("aggregatedNumVertices=" +
                getAggregatedValue(SUM_AGG) +
                " NumVertices=" + getTotalNumVertices());
        if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
                getTotalNumVertices()) {
          throw new RuntimeException("wrong value of SumAggreg: " +
                  getAggregatedValue(SUM_AGG) + ", should be: " +
                  getTotalNumVertices());
        }
        VertexValue maxPagerank = getAggregatedValue(MAX_AGG);
        LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
        VertexValue minPagerank = getAggregatedValue(MIN_AGG);
        LOG.info("aggregatedMinPageRank=" + minPagerank.get());
      }
    }

    @Override
    public void postSuperstep() { }
  }

  /**
   * Master compute associated with {@link PageRankWithKryoSimpleWritable}.
   * It registers required aggregators.
   */
  public static class PageRankWithKryoMasterCompute extends
          DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
            IllegalAccessException {
      registerAggregator(SUM_AGG, LongSumAggregator.class);
      registerPersistentAggregator(MIN_AGG, DoubleMinWrapperAggregator.class);
      registerPersistentAggregator(MAX_AGG, DoubleMaxWrapperAggregator.class);
    }
  }

  /**
   * Simple VertexReader that supports {@link PageRankWithKryoSimpleWritable}
   */
  public static class PageRankWithKryoVertexReader extends
          GeneratedVertexReader<LongWritable, VertexValue, EdgeValue> {
    /** Class logger */
    private static final Logger LOG =
        LoggerFactory.getLogger(
          PageRankWithKryoSimpleWritable.PageRankWithKryoVertexReader.class);

    @Override
    public boolean nextVertex() {
      return totalRecords > recordsRead;
    }

    @Override
    public Vertex<LongWritable, VertexValue, EdgeValue>
    getCurrentVertex() throws IOException {
      Vertex<LongWritable, VertexValue, EdgeValue> vertex =
              getConf().createVertex();
      LongWritable vertexId = new LongWritable(
              (inputSplit.getSplitIndex() * totalRecords) + recordsRead);
      VertexValue vertexValue = new VertexValue(vertexId.get() * 10d);
      long targetVertexId =
              (vertexId.get() + 1) %
                      (inputSplit.getNumSplits() * totalRecords);
      float edgeValue = vertexId.get() * 100f;
      List<Edge<LongWritable, EdgeValue>> edges = Lists.newLinkedList();
      edges.add(EdgeFactory.create(new LongWritable(targetVertexId),
              new EdgeValue(edgeValue)));
      vertex.initialize(vertexId, vertexValue, edges);
      ++recordsRead;
      if (LOG.isInfoEnabled()) {
        LOG.info("next: Return vertexId=" + vertex.getId().get() +
          ", vertexValue=" + vertex.getValue() +
          ", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
      }
      return vertex;
    }
  }

  /**
   *  VertexInputFormat that supports {@link PageRankWithKryoSimpleWritable}
   */
  public static class PageRankWithKryoVertexInputFormat extends
          GeneratedVertexInputFormat<LongWritable, VertexValue, EdgeValue> {
    @Override
    public VertexReader<LongWritable, VertexValue,
            EdgeValue> createVertexReader(InputSplit split,
                                              TaskAttemptContext context)
            throws IOException {
      return new PageRankWithKryoVertexReader();
    }
  }

  /**
   * Creating a custom vertex value class to force kryo to
   * register with a new ID. Please note that a custom
   * class containing a double array is not
   * necessary for the page rank application. It is only
   * used for testing the scenario of kryo encountering an
   * unregistered custom class.
   */
  public static class VertexValue extends KryoSimpleWritable {
    /** Storing the value in an array.
        Double array is an unregistered type
        hence kryo will assign a unique class id */
    private double[] ranks;

    /** Constructor */
    public VertexValue() {
    }

    /**
     * Constructor
     * @param val Vertex value
     */
    public VertexValue(Double val) {
      ranks = new double[1];

      ranks[0] = val;
    }

    /**
     * Get vertex value
     * @return Vertex value
     */
    public Double get() {
      return ranks[0];
    }

    /**
     * Set vertex value.
     * @param val Vertex value
     */
    public void set(Double val) {
      this.ranks[0] = val;
    }
  }

  /**
   * Creating a custom edge value class to force kryo to
   * register with a new ID. Please note that a custom
   * class containing a float is not
   * necessary for the page rank application. It is only
   * used for testing the scenario of kryo encountering an
   * unregistered custom class.
   */
  public static class EdgeValue extends KryoSimpleWritable {
    /** Edge value */
    private Float realValue;

    /** Constructor */
    public EdgeValue() {
    }
    /**
     * Constructor
     * @param val Edge value
     */
    public EdgeValue(Float val) {
      realValue = val;
    }

    /**
     * Get edge value
     * @return Edge value
     */
    public Float get() {
      return realValue;
    }

    /**
     * Set edge value
     * @param val Edge value
     */
    public void set(Float val) {
      this.realValue = val;
    }
  }

  /**
   * Creating a custom message value class to force kryo to
   * register with a new ID. Please note that a custom
   * class containing a double list is not
   * necessary for the page rank application. It is only
   * used for testing the scenario of kryo encountering an
   * unregistered custom class.
   */
  public static class MessageValue extends KryoSimpleWritable {
    /** Storing the message in a list to test the list type */
    private List<Double> msgValue;

    /** Constructor */
    public MessageValue() {
    }

    /**
     * Constructor
     * @param val Message value
     */
    public MessageValue(Double val) {
      msgValue = new ArrayList<>();
      msgValue.add(val);
    }

    /**
     * Get message value
     * @return Message value
     */
    public Double get() {
      return msgValue.get(0);
    }

    /**
     * Set message value
     * @param val Message value
     */
    public void set(Double val) {
      this.msgValue.set(0, val);
    }
  }


  /**
   * Aggregator for getting max double value
   */
  public static class DoubleMaxWrapperAggregator extends
          BasicAggregator<VertexValue> {
    @Override
    public void aggregate(VertexValue value) {
      getAggregatedValue().set(
                      Math.max(getAggregatedValue().get(), value.get()));
    }

    @Override
    public VertexValue createInitialValue() {
      return new VertexValue(Double.NEGATIVE_INFINITY);
    }
  }

  /**
   * Aggregator for getting min double value.
   */
  public static class DoubleMinWrapperAggregator
          extends BasicAggregator<VertexValue> {
    @Override
    public void aggregate(VertexValue value) {
      getAggregatedValue().set(
              Math.min(getAggregatedValue().get(), value.get()));
    }

    @Override
    public VertexValue createInitialValue() {
      return  new VertexValue(Double.MAX_VALUE);
    }
  }

}

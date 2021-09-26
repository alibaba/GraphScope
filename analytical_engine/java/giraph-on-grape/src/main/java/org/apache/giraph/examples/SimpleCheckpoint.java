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

//
// import org.apache.commons.cli.CommandLine;
// import org.apache.commons.cli.CommandLineParser;
// import org.apache.commons.cli.HelpFormatter;
// import org.apache.commons.cli.Options;
// import org.apache.commons.cli.PosixParser;
// import org.apache.giraph.aggregators.LongSumAggregator;
// import org.apache.giraph.graph.BasicComputation;
// import org.apache.giraph.edge.Edge;
// import org.apache.giraph.edge.EdgeFactory;
// import org.apache.giraph.io.formats.FileOutputFormatUtil;
// import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
// import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
// import org.apache.giraph.job.GiraphJob;
// import org.apache.giraph.master.DefaultMasterCompute;
// import org.apache.giraph.graph.Vertex;
// import org.apache.giraph.worker.WorkerContext;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.FloatWritable;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.util.Tool;
// import org.apache.hadoop.util.ToolRunner;
// import org.slf4j.Logger;
//
// import java.io.IOException;
// import org.slf4j.LoggerFactory;
//
/// **
// * An example that simply uses its id, value, and edges to compute new data
// * every iteration to verify that checkpoint restarting works.  Fault injection
// * can also test automated checkpoint restarts.
// */
// hard to support this class!
public class SimpleCheckpoint {}

// implements Tool {
//  /** Which superstep to cause the worker to fail */
//  public static final int FAULTING_SUPERSTEP = 4;
//  /** Vertex id to fault on */
//  public static final long FAULTING_VERTEX_ID = 1;
//  /** Dynamically set number of supersteps */
//  public static final String SUPERSTEP_COUNT =
//      "simpleCheckpointVertex.superstepCount";
//  /** Should fault? */
//  public static final String ENABLE_FAULT =
//      "simpleCheckpointVertex.enableFault";
//  /** Class logger */
//  private static final Logger LOG =
//      LoggerFactory.getLogger(SimpleCheckpoint.class);
//  /** Configuration */
//  private Configuration conf;
//
//  /**
//   * Actual computation.
//   */
//  public static class SimpleCheckpointComputation extends
//      BasicComputation<LongWritable, IntWritable, FloatWritable,
//          FloatWritable> {
//    @Override
//    public void compute(
//        Vertex<LongWritable, IntWritable, FloatWritable> vertex,
//        Iterable<FloatWritable> messages) throws IOException {
//      SimpleCheckpointVertexWorkerContext workerContext = getWorkerContext();
//
//      boolean enableFault = workerContext.getEnableFault();
//      int supersteps = workerContext.getSupersteps();
//
//      if (enableFault && (getSuperstep() == FAULTING_SUPERSTEP) &&
//          (getContext().getTaskAttemptID().getId() == 0) &&
//          (vertex.getId().get() == FAULTING_VERTEX_ID)) {
//        LOG.info("compute: Forced a fault on the first " +
//            "attempt of superstep " +
//            FAULTING_SUPERSTEP + " and vertex id " +
//            FAULTING_VERTEX_ID);
//        System.exit(-1);
//      }
//      if (getSuperstep() > supersteps) {
//        vertex.voteToHalt();
//        return;
//      }
//      long sumAgg = this.<LongWritable>getAggregatedValue(
//          LongSumAggregator.class.getName()).get();
//      LOG.info("compute: " + sumAgg);
//      aggregate(LongSumAggregator.class.getName(),
//          new LongWritable(vertex.getId().get()));
//      LOG.info("compute: sum = " + sumAgg +
//          " for vertex " + vertex.getId());
//      float msgValue = 0.0f;
//      for (FloatWritable message : messages) {
//        float curMsgValue = message.get();
//        msgValue += curMsgValue;
//        LOG.info("compute: got msgValue = " + curMsgValue +
//            " for vertex " + vertex.getId() +
//            " on superstep " + getSuperstep());
//      }
//      int vertexValue = vertex.getValue().get();
//      vertex.setValue(new IntWritable(vertexValue + (int) msgValue));
//      LOG.info("compute: vertex " + vertex.getId() +
//          " has value " + vertex.getValue() +
//          " on superstep " + getSuperstep());
//      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
//        FloatWritable newEdgeValue = new FloatWritable(edge.getValue().get() +
//            (float) vertexValue);
//        Edge<LongWritable, FloatWritable> newEdge =
//            EdgeFactory.create(edge.getTargetVertexId(), newEdgeValue);
//        LOG.info("compute: vertex " + vertex.getId() +
//            " sending edgeValue " + edge.getValue() +
//            " vertexValue " + vertexValue +
//            " total " + newEdgeValue +
//            " to vertex " + edge.getTargetVertexId() +
//            " on superstep " + getSuperstep());
//        vertex.addEdge(newEdge);
//        sendMessage(edge.getTargetVertexId(), newEdgeValue);
//      }
//    }
//  }
//
//  /**
//   * Worker context associated with {@link SimpleCheckpoint}.
//   */
//  public static class SimpleCheckpointVertexWorkerContext
//      extends WorkerContext {
//    /** Filename to indicate whether a fault was found */
//    public static final String FAULT_FILE = "/tmp/faultFile";
//    /** User can access this after the application finishes if local */
//    private static long FINAL_SUM;
//    /** Number of supersteps to run (6 by default) */
//    private int supersteps = 6;
//    /** Enable the fault at the particular vertex id and superstep? */
//    private boolean enableFault = false;
//
//    public static long getFinalSum() {
//      return FINAL_SUM;
//    }
//
//    @Override
//    public void preApplication()
//      throws InstantiationException, IllegalAccessException {
//      supersteps = getContext().getConfiguration()
//          .getInt(SUPERSTEP_COUNT, supersteps);
//      enableFault = getContext().getConfiguration()
//          .getBoolean(ENABLE_FAULT, false);
//    }
//
//    @Override
//    public void postApplication() {
//      setFinalSum(this.<LongWritable>getAggregatedValue(
//          LongSumAggregator.class.getName()).get());
//      LOG.info("FINAL_SUM=" + FINAL_SUM);
//    }
//
//    /**
//     * Set the final sum
//     *
//     * @param value sum
//     */
//    private static void setFinalSum(long value) {
//      FINAL_SUM = value;
//    }
//
//    @Override
//    public void preSuperstep() {
//    }
//
//    @Override
//    public void postSuperstep() { }
//
//    public int getSupersteps() {
//      return this.supersteps;
//    }
//
//    public boolean getEnableFault() {
//      return this.enableFault;
//    }
//  }
//
//  @Override
//  public int run(String[] args) throws Exception {
//    Options options = new Options();
//    options.addOption("h", "help", false, "Help");
//    options.addOption("v", "verbose", false, "Verbose");
//    options.addOption("w",
//        "workers",
//        true,
//        "Number of workers");
//    options.addOption("s",
//        "supersteps",
//        true,
//        "Supersteps to execute before finishing");
//    options.addOption("w",
//        "workers",
//        true,
//        "Minimum number of workers");
//    options.addOption("o",
//        "outputDirectory",
//        true,
//        "Output directory");
//    HelpFormatter formatter = new HelpFormatter();
//    if (args.length == 0) {
//      formatter.printHelp(getClass().getName(), options, true);
//      return 0;
//    }
//    CommandLineParser parser = new PosixParser();
//    CommandLine cmd = parser.parse(options, args);
//    if (cmd.hasOption('h')) {
//      formatter.printHelp(getClass().getName(), options, true);
//      return 0;
//    }
//    if (!cmd.hasOption('w')) {
//      LOG.info("Need to choose the number of workers (-w)");
//      return -1;
//    }
//    if (!cmd.hasOption('o')) {
//      LOG.info("Need to set the output directory (-o)");
//      return -1;
//    }
//
//    GiraphJob bspJob = new GiraphJob(getConf(), getClass().getName());
//    bspJob.getConfiguration().setComputationClass(
//        SimpleCheckpointComputation.class);
//    bspJob.getConfiguration().setVertexInputFormatClass(
//        GeneratedVertexInputFormat.class);
//    bspJob.getConfiguration().setVertexOutputFormatClass(
//        IdWithValueTextOutputFormat.class);
//    bspJob.getConfiguration().setWorkerContextClass(
//        SimpleCheckpointVertexWorkerContext.class);
//    bspJob.getConfiguration().setMasterComputeClass(
//        SimpleCheckpointVertexMasterCompute.class);
//    int minWorkers = Integer.parseInt(cmd.getOptionValue('w'));
//    int maxWorkers = Integer.parseInt(cmd.getOptionValue('w'));
//    bspJob.getConfiguration().setWorkerConfiguration(
//        minWorkers, maxWorkers, 100.0f);
//
//    FileOutputFormatUtil.setOutputPath(bspJob.getInternalJob(),
//                                   new Path(cmd.getOptionValue('o')));
//    boolean verbose = false;
//    if (cmd.hasOption('v')) {
//      verbose = true;
//    }
//    if (cmd.hasOption('s')) {
//      getConf().setInt(SUPERSTEP_COUNT,
//          Integer.parseInt(cmd.getOptionValue('s')));
//    }
//    if (bspJob.run(verbose)) {
//      return 0;
//    } else {
//      return -1;
//    }
//  }
//
//  /**
//   * Master compute associated with {@link SimpleCheckpoint}.
//   * It registers required aggregators.
//   */
//  public static class SimpleCheckpointVertexMasterCompute extends
//      DefaultMasterCompute {
//    @Override
//    public void initialize() throws InstantiationException,
//        IllegalAccessException {
//      registerAggregator(LongSumAggregator.class.getName(),
//          LongSumAggregator.class);
//    }
//  }
//
//  /**
//   * Executable from the command line.
//   *
//   * @param args Command line args.
//   * @throws Exception
//   */
//  public static void main(String[] args) throws Exception {
//    System.exit(ToolRunner.run(new SimpleCheckpoint(), args));
//  }
//
//  @Override
//  public Configuration getConf() {
//    return conf;
//  }
//
//  @Override
//  public void setConf(Configuration conf) {
//    this.conf = conf;
//  }
// }

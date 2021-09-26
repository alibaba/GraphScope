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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.Vertex;
//import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Fully runnable example of how to
 * emit worker data to HDFS during a graph
 * computation.
 */
public class SimpleVertexWithWorkerContext implements Tool {
  /** Directory name of where to write. */
  public static final String OUTPUTDIR = "svwwc.outputdir";
  /** Halting condition for the number of supersteps */
  private static final int TESTLENGTH = 30;
  /** Configuration */
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Actual vetex implementation
   */
  public static class SimpleComputation extends BasicComputation<LongWritable,
      IntWritable, FloatWritable, DoubleWritable> {
    @Override
    public void compute(
        Vertex<LongWritable, IntWritable, FloatWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {

      long superstep = getSuperstep();

      if (superstep < TESTLENGTH) {
        EmitterWorkerContext emitter = (EmitterWorkerContext) getWorkerContext();
        emitter.emit("vertexId=" + vertex.getId() +
            " superstep=" + superstep + "\n");
      } else {
        vertex.voteToHalt();
      }
    }
  }

  /**
   * Example worker context to emit data as part of a superstep.
   */
  @SuppressWarnings("rawtypes")
  public static class EmitterWorkerContext extends WorkerContext {
    /** File name prefix */
    private static final String FILENAME = "emitter_";
    /** Output stream to dump the strings. */
    private DataOutputStream out;

    @Override
    public void preApplication() {
      Context context = getContext();
      FileSystem fs;

      try {
        fs = FileSystem.get(context.getConfiguration());

        String p = context.getConfiguration()
            .get(SimpleVertexWithWorkerContext.OUTPUTDIR);
        if (p == null) {
          throw new IllegalArgumentException(
              SimpleVertexWithWorkerContext.OUTPUTDIR +
              " undefined!");
        }

        Path path = new Path(p);
        if (!fs.exists(path)) {
          throw new IllegalArgumentException(path +
              " doesn't exist");
        }

        Path outF = new Path(path, FILENAME +
            context.getTaskAttemptID());
        if (fs.exists(outF)) {
          throw new IllegalArgumentException(outF +
              " aready exists");
        }

        out = fs.create(outF);
      } catch (IOException e) {
        throw new RuntimeException(
            "can't initialize WorkerContext", e);
      }
    }

    @Override
    public void postApplication() {
      if (out != null) {
        try {
          out.flush();
          out.close();
        } catch (IOException e) {
          throw new RuntimeException(
              "can't finalize WorkerContext", e);
        }
        out = null;
      }
    }

    @Override
    public void preSuperstep() { }

    @Override
    public void postSuperstep() { }

    /**
     * Write this string to the output stream.
     *
     * @param s String to dump.
     */
    public void emit(String s) {
      try {
        out.writeUTF(s);
      } catch (IOException e) {
        throw new RuntimeException("can't emit", e);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "run: Must have 2 arguments <output path> <# of workers>");
    }
//    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
//    job.getConfiguration().setComputationClass(SimpleComputation.class);
//    job.getConfiguration().setVertexInputFormatClass(
//        SimpleSuperstepVertexInputFormat.class);
//    job.getConfiguration().setWorkerContextClass(EmitterWorkerContext.class);
//    job.getConfiguration().set(
//        SimpleVertexWithWorkerContext.OUTPUTDIR, args[0]);
//    job.getConfiguration().setWorkerConfiguration(Integer.parseInt(args[1]),
//        Integer.parseInt(args[1]),
//        100.0f);
//    if (job.run(true)) {
//      return 0;
//    } else {
//      return -1;
//    }
    return 0;
  }

  /**
   * Executable from the command line.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SimpleVertexWithWorkerContext(), args));
  }
}

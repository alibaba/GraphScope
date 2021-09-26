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

package org.apache.giraph.io.formats;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_OUTPUT_FORMAT_SUBDIR;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_OUTPUT_PATH;

import java.io.IOException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class that users should subclass to use their own text based vertex output format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexOutputFormat<I, V, E> {

    private static Logger logger = LoggerFactory.getLogger(TextVertexOutputFormat.class);

    /** Uses the TextOutputFormat to do everything */
    /**
     * Giraph made this file protected here, so we need to provide this
     */
    protected GiraphTextOutputFormat textOutputFormat =
        new GiraphTextOutputFormat() {
            @Override
            protected String getSubdir() {
                return VERTEX_OUTPUT_FORMAT_SUBDIR.get(getConf());
            }

            @Override
            protected String getOutputFileName() {
                return VERTEX_OUTPUT_PATH.get(getConf()) + "-" + getConf().getWorkerId();
            }
        };

    @Override
    public void checkOutputSpecs(JobContext context)
        throws IOException, InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        throw new NotImplementedException();
    }

    /**
     * The factory method which produces the {@link TextVertexWriter} used by this output format.
     *
     * @param context the information about the task
     * @return the text vertex writer to be used
     */
    @Override
    public abstract TextVertexWriter createVertexWriter(TaskAttemptContext
        context) throws IOException, InterruptedException;

    /**
     * Abstract class to be implemented by the user based on their specific vertex output.  Easiest
     * to ignore the key value separator and only use key instead.
     */
    protected abstract class TextVertexWriter
        extends VertexWriter<I, V, E> {

        /**
         * Internal line record writer
         */
        private RecordWriter<Text, Text> lineRecordWriter;
        /**
         * Context passed to initialize
         */
        private TaskAttemptContext context;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
            InterruptedException {
            lineRecordWriter = createLineRecordWriter(context);
            this.context = context;
        }

        /**
         * Create the line record writer. Override this to use a different underlying record writer
         * (useful for testing).
         *
         * @param context the context passed to initialize
         * @return the record writer to be used
         * @throws IOException          exception that can be thrown during creation
         * @throws InterruptedException exception that can be thrown during creation
         */
        protected RecordWriter<Text, Text> createLineRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
            return textOutputFormat.getRecordWriter(context);
        }

        @Override
        public void close(TaskAttemptContext context) throws
            InterruptedException, IOException {
            lineRecordWriter.close(context);
        }

        /**
         * Get the line record writer.
         *
         * @return Record writer to be used for writing.
         */
        public RecordWriter<Text, Text> getRecordWriter() {
            return lineRecordWriter;
        }

        /**
         * Get the context.
         *
         * @return Context passed to initialize.
         */
        public TaskAttemptContext getContext() {
            return context;
        }
    }

    /**
     * Abstract class to be implemented by the user to write a line for each vertex.
     */
    protected abstract class TextVertexWriterToEachLine extends TextVertexWriter {

        @SuppressWarnings("unchecked")
        @Override
        public final void writeVertex(Vertex vertex) throws
            IOException, InterruptedException {
            // Note we are writing line as key with null value
            getRecordWriter().write(convertVertexToLine(vertex), null);
        }

        /**
         * Writes a line for the given vertex.
         *
         * @param vertex the current vertex for writing
         * @return the text line to be written
         * @throws IOException exception that can be thrown while writing
         */
        protected abstract Text convertVertexToLine(Vertex<I, V, E> vertex)
            throws IOException;
    }
}

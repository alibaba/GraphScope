/*
 * The file GiraphConstants.java is referred and derived from
 * project apache/Giraph,
 *
 *    https://github.com/apache/giraph
 * giraph-core/src/main/java/org/apache/giraph/conf/GiraphConstants.java
 *
 * which has the following license:
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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Abstract class that users should subclass to use their own text based vertex input format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexInputFormat<
                I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexInputFormat<I, V, E> {

    // In our implementation, we read from local file.
    // This private is not public, we access with reflection.
    // We need to set this filed before call initialize.
    private BufferedReader fileReader;

    @Override
    public void checkInputSpecs(Configuration conf) {}

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
            throws IOException, InterruptedException {
        // Ignore the hint of numWorkers here since we are using
        // GiraphTextInputFormat to do this for us
        //        return textInputFormat.getVertexSplits(context);
        return null;
    }

    /**
     * The factory method which produces the {@link TextVertexReader} used by this input format.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return the text vertex reader to be used
     */
    @Override
    public abstract TextVertexReader createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException;

    /**
     * Abstract class to be implemented by the user based on their specific vertex input. Easiest to
     * ignore the key value separator and only use key instead.
     * <p>
     * When reading a vertex from each line, extend {@link TextVertexReaderFromEachLine}. If you
     * need to preprocess each line first, then extend {@link TextVertexReaderFromEachLineProcessed}.
     * If you need common exception handling while preprocessing, then extend {@link
     * TextVertexReaderFromEachLineProcessedHandlingExceptions}.
     */
    protected abstract class TextVertexReader extends VertexReader<I, V, E> {

        /**
         * Internal line record reader
         */
        private RecordReader<LongWritable, Text> lineRecordReader;
        /**
         * Context passed to initialize
         */
        private TaskAttemptContext context;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.context = context;
            lineRecordReader = createLineRecordReader(inputSplit, context);
            lineRecordReader.initialize(inputSplit, context);
            if (lineRecordReader instanceof FileRecordReader && Objects.nonNull(fileReader)) {
                ((FileRecordReader) lineRecordReader).setReader(fileReader);
            } else {
                throw new IllegalStateException("not TextLineRecordReader or file reader not set");
            }
        }

        /**
         * Create the line record reader. Override this to use a different underlying record reader
         * (useful for testing).
         *
         * @param inputSplit the split to read
         * @param context    the context passed to initialize
         * @return the record reader to be used
         * @throws IOException          exception that can be thrown during creation
         * @throws InterruptedException exception that can be thrown during creation
         */
        protected RecordReader<LongWritable, Text> createLineRecordReader(
                InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new FileRecordReader();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        /**
         * Get the line record reader.
         *
         * @return Record reader to be used for reading.
         */
        protected RecordReader<LongWritable, Text> getRecordReader() {
            return lineRecordReader;
        }

        /**
         * Get the context.
         *
         * @return Context passed to initialize.
         */
        protected TaskAttemptContext getContext() {
            return context;
        }
    }

    /**
     * Abstract class to be implemented by the user to read a vertex from each text line.
     */
    protected abstract class TextVertexReaderFromEachLine extends TextVertexReader {

        @Override
        public final Vertex<I, V, E> getCurrentVertex() throws IOException, InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            Vertex<I, V, E> vertex = getConf().createVertex();
            vertex.initialize(getId(line), getValue(line), getEdges(line));
            return vertex;
        }

        @Override
        public final boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        /**
         * Reads vertex id from the current line.
         *
         * @param line the current line
         * @return the vertex id corresponding to the line
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract I getId(Text line) throws IOException;

        /**
         * Reads vertex value from the current line.
         *
         * @param line the current line
         * @return the vertex value corresponding to the line
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract V getValue(Text line) throws IOException;

        /**
         * Reads edges value from the current line.
         *
         * @param line the current line
         * @return the edges
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract Iterable<Edge<I, E>> getEdges(Text line) throws IOException;
    }

    /**
     * Abstract class to be implemented by the user to read a vertex from each text line after
     * preprocessing it.
     *
     * @param <T> The resulting type of preprocessing.
     */
    protected abstract class TextVertexReaderFromEachLineProcessed<T> extends TextVertexReader {

        @Override
        public final boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public final Vertex<I, V, E> getCurrentVertex() throws IOException, InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            Vertex<I, V, E> vertex;
            T processed = preprocessLine(line);
            vertex = getConf().createVertex();
            vertex.initialize(getId(processed), getValue(processed), getEdges(processed));
            return vertex;
        }

        /**
         * Preprocess the line so other methods can easily read necessary information for creating
         * vertex.
         *
         * @param line the current line to be read
         * @return the preprocessed object
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract T preprocessLine(Text line) throws IOException;

        /**
         * Reads vertex id from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the vertex id
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract I getId(T line) throws IOException;

        /**
         * Reads vertex value from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the vertex value
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract V getValue(T line) throws IOException;

        /**
         * Reads edges from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the edges
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract Iterable<Edge<I, E>> getEdges(T line) throws IOException;
    }

    // CHECKSTYLE: stop RedundantThrows

    /**
     * Abstract class to be implemented by the user to read a vertex from each text line after
     * preprocessing it with exception handling.
     *
     * @param <T> The resulting type of preprocessing.
     * @param <X> The exception type that can be thrown due to preprocessing.
     */
    protected abstract class TextVertexReaderFromEachLineProcessedHandlingExceptions<
                    T, X extends Throwable>
            extends TextVertexReader {

        @Override
        public final boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public final Vertex<I, V, E> getCurrentVertex() throws IOException, InterruptedException {
            // Note we are reading from value only since key is the line number
            Text line = getRecordReader().getCurrentValue();
            Vertex<I, V, E> vertex;
            T processed = null;
            try {
                processed = preprocessLine(line);
                vertex = getConf().createVertex();
                vertex.initialize(getId(processed), getValue(processed), getEdges(processed));
            } catch (IOException e) {
                throw e;
                // CHECKSTYLE: stop IllegalCatch
            } catch (Throwable t) {
                return handleException(line, processed, (X) t);
                // CHECKSTYLE: resume IllegalCatch
            }
            return vertex;
        }

        /**
         * Preprocess the line so other methods can easily read necessary information for creating
         * vertex.
         *
         * @param line the current line to be read
         * @return the preprocessed object
         * @throws X           exception that can be thrown while preprocessing the line
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract T preprocessLine(Text line) throws X, IOException;

        /**
         * Reads vertex id from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the vertex id
         * @throws X           exception that can be thrown while reading the preprocessed object
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract I getId(T line) throws X, IOException;

        /**
         * Reads vertex value from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the vertex value
         * @throws X           exception that can be thrown while reading the preprocessed object
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract V getValue(T line) throws X, IOException;

        /**
         * Reads edges from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the edges
         * @throws X           exception that can be thrown while reading the preprocessed object
         * @throws IOException exception that can be thrown while reading
         */
        protected abstract Iterable<Edge<I, E>> getEdges(T line) throws X, IOException;

        /**
         * Handles exceptions while reading vertex from each line.
         *
         * @param line      the line that was being read when the exception was thrown
         * @param processed the object obtained by preprocessing the line. Can be null if exception
         *                  was thrown during preprocessing.
         * @param e         the exception thrown while reading the line
         * @return the recovered/alternative vertex to be used
         */
        protected Vertex<I, V, E> handleException(Text line, T processed, X e) {
            throw new IllegalArgumentException(e);
        }
    }
    // CHECKSTYLE: resume RedundantThrows

}

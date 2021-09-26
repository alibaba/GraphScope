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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Abstract class that users should subclass to use their own text based
 * edge output format.
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public abstract class TextEdgeInputFormat<I extends WritableComparable,
    E extends Writable> extends EdgeInputFormat<I, E> {
    /** Underlying GiraphTextInputFormat. */
//    protected GiraphTextInputFormat textInputFormat = new GiraphTextInputFormat();
    //In our implementation, we read from local file.
    //This private is not public, we access with reflection.
    //We need to set this filed before call initialize.
    private BufferedReader fileReader;

    @Override public void checkInputSpecs(Configuration conf) { }

    @Override
    public List<InputSplit> getSplits(
        JobContext context, int minSplitCountHint) throws IOException,
        InterruptedException {
        // Ignore the hint of numWorkers here since we are using
        // GiraphTextInputFormat to do this for us
//        return textInputFormat.getEdgeSplits(context);
        return null;
    }

    /**
     * {@link EdgeReader} for {@link TextEdgeInputFormat}.
     */
    protected abstract class TextEdgeReader extends EdgeReader<I, E> {
        /** Internal line record reader */
        private RecordReader<LongWritable, Text> lineRecordReader;
        /** Context passed to initialize */
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
         * Create the line record reader. Override this to use a different
         * underlying record reader (useful for testing).
         *
         * @param inputSplit
         *          the split to read
         * @param context
         *          the context passed to initialize
         * @return
         *         the record reader to be used
         * @throws IOException
         *           exception that can be thrown during creation
         * @throws InterruptedException
         *           exception that can be thrown during creation
         */
        protected RecordReader<LongWritable, Text>
        createLineRecordReader(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
//            return textInputFormat.createRecordReader(inputSplit, context);
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
     * Abstract class to be implemented by the user to read an edge from each
     * text line.
     */
    protected abstract class TextEdgeReaderFromEachLine extends TextEdgeReader {
        @Override
        public final I getCurrentSourceId() throws IOException,
            InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            return getSourceVertexId(line);
        }

        @Override
        public final Edge<I, E> getCurrentEdge() throws IOException,
            InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            I targetVertexId = getTargetVertexId(line);
            E edgeValue = getValue(line);
            return EdgeFactory.create(targetVertexId, edgeValue);
        }

        @Override
        public final boolean nextEdge() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        /**
         * Reads source vertex id from the current line.
         *
         * @param line
         *          the current line
         * @return
         *         the source vertex id corresponding to the line
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract I getSourceVertexId(Text line) throws IOException;


        /**
         * Reads target vertex id from the current line.
         *
         * @param line
         *          the current line
         * @return
         *         the target vertex id corresponding to the line
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract I getTargetVertexId(Text line) throws IOException;

        /**
         * Reads edge value from the current line.
         *
         * @param line
         *          the current line
         * @return
         *         the edge value corresponding to the line
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract E getValue(Text line) throws IOException;
    }

    /**
     * Abstract class to be implemented by the user to read an edge from each
     * text line after preprocessing it.
     *
     * @param <T>
     *          The resulting type of preprocessing.
     */
    protected abstract class TextEdgeReaderFromEachLineProcessed<T> extends
        TextEdgeReader {
        /** Generic type holding processed line */
        private T processedLine;

        @Override
        public I getCurrentSourceId() throws IOException, InterruptedException {
            T processed = processCurrentLine();
            return getSourceVertexId(processed);
        }

        @Override
        public final Edge<I, E> getCurrentEdge() throws IOException,
            InterruptedException {
            T processed = processCurrentLine();
            I targetVertexId = getTargetVertexId(processed);
            E edgeValue = getValue(processed);
            return EdgeFactory.create(targetVertexId, edgeValue);
        }

        /**
         * Process the current line to the user's type.
         *
         * @return T processed line
         * @throws IOException on I/O error
         * @throws InterruptedException on interruption
         */
        private T processCurrentLine() throws IOException, InterruptedException {
            if (processedLine == null) {
                Text line = getRecordReader().getCurrentValue();
                processedLine = preprocessLine(line);
            }
            return processedLine;
        }

        @Override
        public final boolean nextEdge() throws IOException, InterruptedException {
            processedLine = null;
            return getRecordReader().nextKeyValue();
        }

        /**
         * Preprocess the line so other methods can easily read necessary
         * information for creating edge
         *
         * @param line
         *          the current line to be read
         * @return
         *         the preprocessed object
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract T preprocessLine(Text line) throws IOException;

        /**
         * Reads target vertex id from the preprocessed line.
         *
         * @param line
         *          the object obtained by preprocessing the line
         * @return
         *         the target vertex id
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract I getTargetVertexId(T line) throws IOException;

        /**
         * Reads source vertex id from the preprocessed line.
         *
         * @param line
         *          the object obtained by preprocessing the line
         * @return
         *         the source vertex id
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract I getSourceVertexId(T line) throws IOException;

        /**
         * Reads edge value from the preprocessed line.
         *
         * @param line
         *          the object obtained by preprocessing the line
         * @return
         *         the edge value
         * @throws IOException
         *           exception that can be thrown while reading
         */
        protected abstract E getValue(T line) throws IOException;
    }
}

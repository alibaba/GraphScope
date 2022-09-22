/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.example.giraph.format;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DatagenEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

    /**
     * Create an edge reader for a given split. The framework will call {@link
     * EdgeReader#initialize(InputSplit, TaskAttemptContext)} before the split is used.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     */
    @Override
    public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new LiveJournalEdgeReader();
    }

    public class LiveJournalEdgeReader extends TextEdgeReaderFromEachLineProcessed<String[]> {

        String SEPARATOR = ",";
        /**
         * Cached vertex id for the current line
         */
        private LongWritable srcId;

        private LongWritable dstId;
        private DoubleWritable edgeValue;

        /**
         * Preprocess the line so other methods can easily read necessary information for creating
         * edge
         *
         * @param line the current line to be read
         * @return the preprocessed object
         * @throws IOException exception that can be thrown while reading
         */
        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(SEPARATOR);
            if (tokens.length != 3) {
                throw new IllegalStateException("expect 3 ele in edge line");
            }
            srcId = new LongWritable(Long.parseLong(tokens[0]));
            dstId = new LongWritable(Long.parseLong(tokens[1]));
            edgeValue = new DoubleWritable(Double.parseDouble(tokens[2]));
            return tokens;
        }

        /**
         * Reads target vertex id from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the target vertex id
         * @throws IOException exception that can be thrown while reading
         */
        @Override
        protected LongWritable getTargetVertexId(String[] line) throws IOException {
            return dstId;
        }

        /**
         * Reads source vertex id from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the source vertex id
         * @throws IOException exception that can be thrown while reading
         */
        @Override
        protected LongWritable getSourceVertexId(String[] line) throws IOException {
            return srcId;
        }

        /**
         * Reads edge value from the preprocessed line.
         *
         * @param line the object obtained by preprocessing the line
         * @return the edge value
         * @throws IOException exception that can be thrown while reading
         */
        @Override
        protected DoubleWritable getValue(String[] line) throws IOException {
            return edgeValue;
        }
    }
}

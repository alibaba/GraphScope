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

package com.alibaba.graphscope.example.giraph.circle;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CircleEdgeInputFormat extends TextEdgeInputFormat<LongWritable, LongWritable> {
    public CircleEdgeInputFormat() {}

    public EdgeReader<LongWritable, LongWritable> createEdgeReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new CircleEdgeInputFormat.P2PEdgeReader();
    }

    public class P2PEdgeReader
            extends TextEdgeInputFormat<LongWritable, LongWritable>
                            .TextEdgeReaderFromEachLineProcessed<
                    String[]> {
        String SEPARATOR = " ";
        private LongWritable srcId;
        private LongWritable dstId;
        private LongWritable edgeValue;

        public P2PEdgeReader() {
            super();
        }

        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(this.SEPARATOR);
            if (tokens.length != 3) {
                throw new IllegalStateException("expect 3 ele in edge line");
            } else {
                this.srcId = new LongWritable(Long.parseLong(tokens[0]));
                this.dstId = new LongWritable(Long.parseLong(tokens[1]));
                this.edgeValue = new LongWritable(Long.parseLong(tokens[2]));
                return tokens;
            }
        }

        protected LongWritable getTargetVertexId(String[] line) throws IOException {
            return this.dstId;
        }

        protected LongWritable getSourceVertexId(String[] line) throws IOException {
            return this.srcId;
        }

        protected LongWritable getValue(String[] line) throws IOException {
            return this.edgeValue;
        }
    }
}

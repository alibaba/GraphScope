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

import com.google.common.collect.Lists;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CircleVertexInputFormat
        extends TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable> {
    public CircleVertexInputFormat() {}

    public TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable>.TextVertexReader
            createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CircleVertexInputFormat.P2PVertexReader();
    }

    public class P2PVertexReader
            extends TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable>
                            .TextVertexReaderFromEachLineProcessed<
                    String[]> {
        String SEPARATOR = " ";
        private LongWritable id;
        private VertexAttrWritable value;

        public P2PVertexReader() {
            super();
        }

        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(this.SEPARATOR);
            this.id = new LongWritable(Long.parseLong(tokens[0]));
            List<MsgWritable> writables = new ArrayList<MsgWritable>();
            writables.add(new MsgWritable(Arrays.asList(1L, 2L, 3L), Arrays.asList(12L, 23L)));
            this.value = new VertexAttrWritable(writables);
            return tokens;
        }

        protected LongWritable getId(String[] tokens) throws IOException {
            return this.id;
        }

        protected VertexAttrWritable getValue(String[] tokens) throws IOException {
            return this.value;
        }

        protected Iterable<Edge<LongWritable, LongWritable>> getEdges(String[] tokens)
                throws IOException {
            List<Edge<LongWritable, LongWritable>> edges = Lists.newArrayListWithCapacity(0);
            return edges;
        }
    }
}

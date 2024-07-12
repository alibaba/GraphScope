//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.graphscope.example.giraph.circle;

import com.alibaba.graphscope.example.giraph.format.VertexAttrWritable;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CircleInputFormat extends TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable> {
    public CircleInputFormat() {
    }

    public TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable>.TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CircleInputFormat.P2PVertexReader();
    }

    public class P2PVertexReader extends TextVertexInputFormat<LongWritable, VertexAttrWritable, LongWritable>.TextVertexReaderFromEachLineProcessed<String[]> {
        String SEPARATOR = ",";
        private LongWritable id;
        private VertexAttrWritable value;

        public P2PVertexReader() {
        }

        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(this.SEPARATOR);
            this.id = new LongWritable(Long.parseLong(tokens[0]));
            this.value = new VertexAttrWritable();
            return tokens;
        }

        protected LongWritable getId(String[] tokens) throws IOException {
            return this.id;
        }

        protected VertexAttrWritable getValue(String[] tokens) throws IOException {
            return this.value;
        }

        protected Iterable<Edge<LongWritable, LongWritable>> getEdges(String[] tokens) throws IOException {
            List<Edge<LongWritable, LongWritable>> edges = Lists.newArrayListWithCapacity(0);
            return edges;
        }
    }
}

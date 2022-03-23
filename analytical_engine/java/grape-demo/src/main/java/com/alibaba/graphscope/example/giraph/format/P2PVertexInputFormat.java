package com.alibaba.graphscope.example.giraph.format;

import com.google.common.collect.Lists;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class P2PVertexInputFormat
        extends TextVertexInputFormat<LongWritable, LongWritable, LongWritable> {

    /**
     * The factory method which produces the {@link TextVertexReader} used by this input format.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return the text vertex reader to be used
     */
    @Override
    public TextVertexInputFormat<LongWritable, LongWritable, LongWritable>.TextVertexReader
            createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new P2PVertexReader();
    }

    public class P2PVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        String SEPARATOR = " ";

        /**
         * Cached vertex id for the current line
         */
        private LongWritable id;

        private LongWritable value;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            //            logger.debug("line: " + line.toString());
            String[] tokens = line.toString().split(SEPARATOR);
            //            logger.debug(String.join(",", tokens));
            id = new LongWritable(Long.parseLong(tokens[0]));
            value = new LongWritable(Long.parseLong(tokens[1]));
            return tokens;
        }

        @Override
        protected LongWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected LongWritable getValue(String[] tokens) throws IOException {
            return value;
        }

        @Override
        protected Iterable<Edge<LongWritable, LongWritable>> getEdges(String[] tokens)
                throws IOException {
            List<Edge<LongWritable, LongWritable>> edges = Lists.newArrayListWithCapacity(0);
            return edges;
        }
    }
}

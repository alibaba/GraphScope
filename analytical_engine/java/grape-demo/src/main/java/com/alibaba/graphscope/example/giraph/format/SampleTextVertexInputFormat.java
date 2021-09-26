package com.alibaba.graphscope.example.giraph.format;

import com.alibaba.graphscope.example.giraph.SccVertexValue;
import com.google.common.collect.Lists;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SampleTextVertexInputFormat
        extends TextVertexInputFormat<LongWritable, SccVertexValue, LongWritable> {

    private static final Logger logger = LoggerFactory.getLogger(SampleTextVertexInputFormat.class);

    /**
     * The factory method which produces the {@link TextVertexReader} used by this input format.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return the text vertex reader to be used
     */
    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new LongLongNullVertexReader();
    }

    /**
     * Vertex reader associated with {@link SampleTextVertexInputFormat}.
     */
    public class LongLongNullVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        String SEPARATOR = ":";

        /**
         * Cached vertex id for the current line
         */
        private LongWritable id;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            logger.debug("line: " + line.toString());
            String[] tokens = line.toString().split(SEPARATOR);
            logger.debug(String.join(",", tokens));
            id = new LongWritable(Long.parseLong(tokens[0]));
            return tokens;
        }

        @Override
        protected LongWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected SccVertexValue getValue(String[] tokens) throws IOException {
            return new SccVertexValue(Long.parseLong(tokens[0]));
        }

        @Override
        protected Iterable<Edge<LongWritable, LongWritable>> getEdges(String[] tokens)
                throws IOException {
            List<Edge<LongWritable, LongWritable>> edges =
                    Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 1; n < tokens.length; n++) {
                edges.add(
                        EdgeFactory.create(
                                new LongWritable(Long.parseLong(tokens[n])),
                                new LongWritable(Long.parseLong(tokens[n]))));
            }
            return edges;
        }
    }
}

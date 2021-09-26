package com.alibaba.graphscope.example.giraph.format;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LiveJournalVertexInputFormat
        extends TextVertexInputFormat<LongWritable, LongWritable, LongWritable> {

    /**
     * The factory method which produces the {@link TextVertexReader} used by this input format.
     *
     * @param split the split to be read
     * @param context the information about the task
     * @return the text vertex reader to be used
     */
    @Override
    public TextVertexInputFormat<LongWritable, LongWritable, LongWritable>.TextVertexReader
            createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new LiveJournalVertexReader();
    }

    public class LiveJournalVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        String SEPARATOR = ",";

        /** Cached vertex id for the current line */
        private LongWritable id;

        private LongWritable value;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = line.toString().split(SEPARATOR);
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

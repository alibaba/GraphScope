package com.alibaba.graphscope.example.giraph.format;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LiveJournalEdgeInputFormat extends TextEdgeInputFormat<LongWritable, LongWritable> {

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
    public EdgeReader<LongWritable, LongWritable> createEdgeReader(
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
        private LongWritable edgeValue;

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
            //            logger.debug("line: " + line.toString());
            String[] tokens = line.toString().split(SEPARATOR);
            if (tokens.length != 3) {
                throw new IllegalStateException("expect 3 ele in edge line");
            }
            //            logger.debug(String.join(",", tokens));
            srcId = new LongWritable(Long.parseLong(tokens[0]));
            dstId = new LongWritable(Long.parseLong(tokens[1]));
            edgeValue = new LongWritable(Long.parseLong(tokens[2]));
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
        protected LongWritable getValue(String[] line) throws IOException {
            return edgeValue;
        }
    }
}

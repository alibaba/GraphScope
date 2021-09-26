package com.alibaba.graphscope.example.giraph;

import java.io.IOException;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Only send msg. */
public class MessageBenchMark
        extends BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(MessageBenchMark.class);

    private static int MAX_SUPER_STEP = Integer.valueOf(System.getenv("MAX_SUPER_STEP"));

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *     message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, LongWritable> vertex,
            Iterable<LongWritable> messages)
            throws IOException {
        if (getSuperstep() >= MAX_SUPER_STEP) {
            vertex.voteToHalt();
            return;
        }
        if (getSuperstep() >= 1) {
            int msgCnt = 0;
            for (LongWritable message : messages) {
                msgCnt += 1;
            }
            // Record number of vertices that has vertices.
            MessageBenchMarkWorkerContext.stepMessageReceived += msgCnt;
            MessageBenchMarkWorkerContext.totalMessageReceived += msgCnt;

            //            if (vertex.getId().get() % 10000000 == 0) {
            //                logger.debug("vertex: {} receive msg size: {}", vertex.getId(),
            // msgCnt);
            //            }
        }

        MessageBenchMarkWorkerContext.stepMessageSent += vertex.getNumEdges();
        MessageBenchMarkWorkerContext.totalMessageSent += vertex.getNumEdges();

        // traverse graph

        LongWritable msg = new LongWritable(vertex.getId().get());
        sendMessageToAllEdges(vertex, msg);
        // logger.info("Vertex [" + vertex.getId() + "] send to all edges " +  vertex.getId());
    }

    public static class MessageBenchMarkWorkerContext extends WorkerContext {

        private static long stepMessageSent = 0;
        private static long totalMessageSent = 0;
        private static long stepMessageReceived = 0;
        private static long totalMessageReceived = 0;

        /**
         * Initialize the WorkerContext. This method is executed once on each Worker before the
         * first superstep starts.
         *
         * @throws IllegalAccessException Thrown for getting the class
         * @throws InstantiationException Expected instantiation in this method.
         */
        @Override
        public void preApplication() throws InstantiationException, IllegalAccessException {
            totalMessageSent = 0;
            totalMessageReceived = 0;
        }

        /**
         * Finalize the WorkerContext. This method is executed once on each Worker after the last
         * superstep ends.
         */
        @Override
        public void postApplication() {
            logger.info(
                    "after application: msg sent: "
                            + totalMessageSent
                            + ", msg rcv: "
                            + totalMessageReceived);
        }

        /**
         * Execute user code. This method is executed once on each Worker before each superstep
         * starts.
         */
        @Override
        public void preSuperstep() {
            stepMessageSent = 0;
            stepMessageReceived = 0;
        }

        /**
         * Execute user code. This method is executed once on each Worker after each superstep ends.
         */
        @Override
        public void postSuperstep() {
            logger.info(
                    "after superstep: "
                            + getSuperstep()
                            + "msg sent: "
                            + stepMessageSent
                            + ", msg rcv: "
                            + stepMessageReceived);
        }
    }
}

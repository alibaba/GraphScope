package com.alibaba.graphscope.example.giraph;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Only send msg.
 */
public class MessageApp
        extends BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

    public static LongConfOption MAX_SUPER_STEP;
    private static Logger logger = LoggerFactory.getLogger(MessageApp.class);

    static {
        String maxSuperStep = System.getenv("MAX_SUPER_STEP");
        if (Objects.isNull(maxSuperStep) || maxSuperStep.isEmpty()) {
            MAX_SUPER_STEP = new LongConfOption("maxSuperStep", 1, "max super step");
        } else {
            MAX_SUPER_STEP =
                    new LongConfOption(
                            "maxSuperStep", Long.valueOf(maxSuperStep), "max super step");
        }
    }

    /**
     * Must be defined by user to do computation on a single Vertex.
     *
     * @param vertex   Vertex
     * @param messages Messages that were sent to this vertex in the previous superstep. Each
     *                 message is only guaranteed to have
     */
    @Override
    public void compute(
            Vertex<LongWritable, LongWritable, LongWritable> vertex,
            Iterable<LongWritable> messages)
            throws IOException {
        if (getSuperstep() == 0) {
            logger.info("There should be no messages in step0, " + vertex.getId());
            boolean flag = false;
            for (LongWritable message : messages) {
                flag = true;
            }
            if (flag) {
                throw new IllegalStateException(
                        "Expect no msg received in step 1, but actually received");
            }
            LongWritable msg = new LongWritable(vertex.getId().get());
            sendMessageToAllEdges(vertex, msg);
        } else if (getSuperstep() < MAX_SUPER_STEP.get(getConf())) {
            logger.info("step [{}] Checking received msg", getSuperstep());
            int msgCnt = 0;
            for (LongWritable message : messages) {
                msgCnt += 1;
            }
            vertex.setValue(new LongWritable(msgCnt));
        } else if (getSuperstep() == MAX_SUPER_STEP.get(getConf())) {
            vertex.voteToHalt();
        } else {
            logger.info("Impossible: " + getSuperstep());
        }
    }
}

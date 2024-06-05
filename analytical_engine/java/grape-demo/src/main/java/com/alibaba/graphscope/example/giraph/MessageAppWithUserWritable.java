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

package com.alibaba.graphscope.example.giraph;

import com.alibaba.graphscope.example.giraph.writable.MultipleLongWritable;

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
public class MessageAppWithUserWritable
        extends BasicComputation<
                LongWritable, MultipleLongWritable, MultipleLongWritable, MultipleLongWritable> {

    public static LongConfOption MAX_SUPER_STEP;
    private static Logger logger = LoggerFactory.getLogger(MessageAppWithUserWritable.class);

    static {
        String maxSuperStep = System.getenv("MAX_SUPER_STEP");
        if (Objects.isNull(maxSuperStep) || maxSuperStep.isEmpty()) {
            MAX_SUPER_STEP = new LongConfOption("maxSuperStep", 3, "max super step");
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
            Vertex<LongWritable, MultipleLongWritable, MultipleLongWritable> vertex,
            Iterable<MultipleLongWritable> messages)
            throws IOException {
        if (getSuperstep() == 0) {
            // logger.info("There should be no messages in step0, " + vertex.getId());
            boolean flag = false;
            for (MultipleLongWritable message : messages) {
                flag = true;
            }
            if (flag) {
                throw new IllegalStateException(
                        "Expect no msg received in step 1, but actually received");
            }
            MultipleLongWritable msg = new MultipleLongWritable(vertex.getId().get());
            sendMessageToAllEdges(vertex, msg);
        } else if (getSuperstep() < MAX_SUPER_STEP.get(getConf())) {
            if (vertex.getId().get() < 20) {
                logger.info("step [{}] Checking received msg", getSuperstep());
            }
            int msgCnt = 0;
            for (MultipleLongWritable message : messages) {
                msgCnt += 1;
            }
            vertex.setValue(new MultipleLongWritable(msgCnt));
        } else if (getSuperstep() == MAX_SUPER_STEP.get(getConf())) {
            vertex.voteToHalt();
        } else {
            logger.info("Impossible: " + getSuperstep());
        }
    }
}

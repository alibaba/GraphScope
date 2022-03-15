/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.fragment.IFragment;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessageStoreFactory<I extends WritableComparable, M extends Writable, GS_VID_T>
        implements MessageStoreFactory<I, M, MessageStore<I, M, GS_VID_T>> {

    private static Logger logger = LoggerFactory.getLogger(DefaultMessageStoreFactory.class);

    private IFragment<?, GS_VID_T, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

    /**
     * Creates new message store.
     *
     * @param messageClasses Message classes information to be held in the store
     * @return New message store
     */
    @Override
    public MessageStore<I, M, GS_VID_T> newStore(MessageClasses<I, M> messageClasses) {
        if (conf.usePrimitiveMessageStore()) {
            if (conf.getGrapeVidClass().equals(Long.class)
                    && messageClasses.getMessageClass().equals(DoubleWritable.class)) {
                if (messageClasses.useMessageCombiner()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("creating LongDoubleMessageDoubleMessageStore with combiner");
                    }
                    return (MessageStore<I, M, GS_VID_T>)
                            new LongDoubleMessageStoreWithCombiner(
                                    fragment,
                                    (ImmutableClassesGiraphConfiguration<
                                                    ? extends LongWritable,
                                                    ? extends Writable,
                                                    ? extends Writable>)
                                            conf,
                                    (MessageCombiner<? super LongWritable, DoubleWritable>)
                                            messageClasses.createMessageCombiner(conf));
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info(
                                "creating LongDoubleMessageDoubleMessageStore with no"
                                        + " combiner");
                    }
                    return (MessageStore<I, M, GS_VID_T>)
                            new LongDoubleMessageStore<I>(fragment, conf);
                }
            } else if (conf.getGrapeVidClass().equals(Long.class)
                    && messageClasses.getMessageClass().equals(LongWritable.class)) {
                if (messageClasses.useMessageCombiner()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("creating LongLongMessageStore with combiner");
                    }
                    return (MessageStore<I, M, GS_VID_T>)
                            new LongLongMessageStoreWithCombiner(
                                    fragment,
                                    (ImmutableClassesGiraphConfiguration<
                                                    ? extends LongWritable,
                                                    ? extends Writable,
                                                    ? extends Writable>)
                                            conf,
                                    (MessageCombiner<? super LongWritable, LongWritable>)
                                            messageClasses.createMessageCombiner(conf));
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("creating LongLongMessageStore with no" + " combiner");
                    }
                    return (MessageStore<I, M, GS_VID_T>)
                            new LongLongMessageStore<I>(fragment, conf);
                }
            } else {
                throw new IllegalStateException(
                        "Not supported primitve store: vid:"
                                + conf.getGrapeVidClass().getSimpleName()
                                + ", msg:"
                                + messageClasses.getMessageClass().getSimpleName());
            }
        } else {
            return new DefaultMessageStore<I, M, GS_VID_T>(fragment, conf);
        }
    }

    /**
     * Implementation class should use this method of initialization of any required internal
     * state.
     *
     * @param fragment fragment used for partition querying
     * @param conf     Configuration
     */
    @Override
    public void initialize(IFragment fragment, ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
    }
}

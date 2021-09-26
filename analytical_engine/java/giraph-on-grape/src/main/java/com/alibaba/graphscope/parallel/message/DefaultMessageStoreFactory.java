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

import java.util.Objects;

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
        String messageStoreType = System.getenv("MESSAGE_STORE_TYPE");
        if (Objects.nonNull(messageStoreType)) {
            if (messageStoreType.equals("simple")) {
                return new SimpleMessageStore<>(fragment, conf);
            } else if (messageStoreType.equals("primitive")) {
                // try to use primitive store for better performace.
                if (conf.getGrapeVidClass().equals(Long.class)
                        && messageClasses.getMessageClass().equals(DoubleWritable.class)) {
                    if (messageClasses.useMessageCombiner()) {
                        if (logger.isInfoEnabled()) {
                            logger.info(
                                    "creating LongDoubleMessageDoubleMessageStore with combiner");
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
                            logger.info(
                                "creating LongLongMessageStore with combiner");
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
                            logger.info(
                                "creating LongLongMessageStore with no"
                                    + " combiner");
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
            }
        }
        throw new IllegalStateException("unrecognizable message store" + messageStoreType);
    }

    /**
     * Implementation class should use this method of initialization of any required internal state.
     *
     * @param fragment fragment used for partition querying
     * @param conf Configuration
     */
    @Override
    public void initialize(IFragment fragment, ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
    }
}

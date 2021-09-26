package com.alibaba.graphscope.parallel.mm.impl;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

import com.alibaba.graphscope.communication.FFICommunicator;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.message.MessageStoreFactory;
import com.alibaba.graphscope.parallel.mm.GiraphMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * skeletal implementation for giraph message manager. providing common utils for message
 * store(cache).
 *
 * @param <OID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 * @param <IN_MSG_T>
 * @param <OUT_MSG_T>
 * @param <GS_VID_T>
 * @param <GS_OID_T>
 */
public abstract class AbstractMessageManager<
                OID_T extends WritableComparable,
                VDATA_T extends Writable,
                EDATA_T extends Writable,
                IN_MSG_T extends Writable,
                OUT_MSG_T extends Writable,
                GS_VID_T,
                GS_OID_T>
        implements GiraphMessageManager<
                OID_T, VDATA_T, EDATA_T, IN_MSG_T, OUT_MSG_T, GS_VID_T, GS_OID_T> {

    protected IFragment<GS_OID_T, GS_VID_T, ?, ?> fragment;
    protected DefaultMessageManager grapeMessager;
    protected com.alibaba.graphscope.ds.Vertex<GS_VID_T> grapeVertex;
    /**
     * A FFICommunicator should be only used by netty mm, mpi mm never use them.
     * */
    private FFICommunicator communicator;
    protected int fragId;
    protected int fragNum;
    private long maxInnerVertexLid;

    private MessageStoreFactory<OID_T, IN_MSG_T, MessageStore<OID_T, IN_MSG_T, GS_VID_T>>
            messageStoreFactory;
    protected volatile MessageStore<OID_T, IN_MSG_T, GS_VID_T> nextIncomingMessageStore;
    protected volatile MessageStore<OID_T, IN_MSG_T, GS_VID_T> currentIncomingMessageStore;

    private ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf;

    public ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> getConf() {
        return conf;
    }

    public IFragment<GS_OID_T, GS_VID_T, ?, ?> getFragment() {
        return fragment;
    }

    public FFICommunicator getCommunicator() {
        return communicator;
    }

    public AbstractMessageManager(
            IFragment<GS_OID_T, GS_VID_T, ?, ?> fragment,
            DefaultMessageManager mm,
            ImmutableClassesGiraphConfiguration<OID_T, VDATA_T, EDATA_T> conf,
            FFICommunicator communicator) {
        this.communicator = communicator;
        this.conf = conf;
        this.fragment = fragment;
        this.grapeMessager = mm;
        this.grapeVertex =
                (com.alibaba.graphscope.ds.Vertex<GS_VID_T>)
                        FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
        this.fragId = fragment.fid();
        this.fragNum = fragment.fnum();
        this.maxInnerVertexLid = fragment.getInnerVerticesNum();

        messageStoreFactory = createMessageStoreFactory();
        nextIncomingMessageStore = messageStoreFactory.newStore(conf.getIncomingMessageClasses());
        currentIncomingMessageStore =
                messageStoreFactory.newStore(conf.getIncomingMessageClasses());
    }

    private MessageStoreFactory<OID_T, IN_MSG_T, MessageStore<OID_T, IN_MSG_T, GS_VID_T>>
            createMessageStoreFactory() {
        Class<? extends MessageStoreFactory> messageStoreFactoryClass =
                MESSAGE_STORE_FACTORY_CLASS.get(conf);

        MessageStoreFactory messageStoreFactoryInstance =
                ReflectionUtils.newInstance(messageStoreFactoryClass);
        messageStoreFactoryInstance.initialize(fragment, conf);

        return messageStoreFactoryInstance;
    }

    /**
     * Get the messages received from last round.
     *
     * @param lid local id.
     * @return received msg.
     */
    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        checkLid(lid);
        return currentIncomingMessageStore.getMessages(lid);
    }

    /**
     * Check any message available on this vertex.
     *
     * @param lid local id
     * @return true if recevied messages.
     */
    @Override
    public boolean messageAvailable(long lid) {
        checkLid(lid);
        return currentIncomingMessageStore.messageAvailable(lid);
    }

    /**
     * As this is called after superStep and before presuperStep's swapping, we check
     * nextIncomingMessage Store.
     *
     * @return true if message received
     */
    @Override
    public boolean anyMessageReceived() {
        return currentIncomingMessageStore.anyMessageReceived();
    }

    @Override
    public void forceContinue() {
        grapeMessager.ForceContinue();
    }

    protected void checkLid(long lid) {
        if (lid >= maxInnerVertexLid) {
            throw new IndexOutOfBoundsException("lid: " + lid + " max lid " + maxInnerVertexLid);
        }
    }
}

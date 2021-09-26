package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A simple in memory message store with map. It
 *
 * @param <OID_T> giraph oid
 * @param <IN_MSG_T> incoming msg type
 * @param <GS_VID_T> graphscope vid type
 */
public class SimpleMessageStore<
                OID_T extends WritableComparable, IN_MSG_T extends Writable, GS_VID_T>
        implements MessageStore<OID_T, IN_MSG_T, GS_VID_T> {
    private static Logger logger = LoggerFactory.getLogger(SimpleMessageStore.class);

    /** lid -> messages. */
    private Map<GS_VID_T, List<Writable>> messages;

    private IFragment<?, GS_VID_T, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration conf;
    private Vertex<GS_VID_T> vertex;

    public SimpleMessageStore(IFragment fragment, ImmutableClassesGiraphConfiguration conf) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
        messages = new HashMap<>();
    }

    @Override
    public void addLidMessage(GS_VID_T lid, IN_MSG_T writable) {
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>());
        }
        messages.get(lid).add(writable);
    }

    @Override
    public void addGidMessages(
            Iterator<GS_VID_T> gidIterator, Iterator<IN_MSG_T> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            GS_VID_T gid = gidIterator.next();
            boolean res = fragment.gid2Vertex(gid, vertex);
            if (!res) {
                throw new IllegalStateException("convert gid: " + gid + " to lid failed: ");
            }
            Writable msg = writableIterator.next();
            GS_VID_T lid = vertex.GetValue();
            if (!messages.containsKey(lid)) {
                messages.put(lid, new ArrayList<>());
            }
            messages.get(vertex.GetValue()).add(msg);
            cnt += 1;
        }
        logger.info("worker [" + fragment.fid() + "] messages to self cnt: " + cnt);
    }

    @Override
    public synchronized void addGidMessage(GS_VID_T gid, IN_MSG_T writable) {
        vertex.SetValue(gid);
        boolean res = fragment.gid2Vertex(gid, vertex);
        GS_VID_T lid = vertex.GetValue();
        //        logger.debug("Inserting gid message: (" + gid + ", " +writable + ")");
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>());
        }
        messages.get(lid).add(writable);
        //        logger.debug("after inserting, gid: " + gid +  ",lid: " +  lid + " message:" +
        // messages);
    }

    @Override
    public void swap(MessageStore<OID_T, IN_MSG_T, GS_VID_T> other) {
        if (other instanceof SimpleMessageStore) {
            SimpleMessageStore simpleMessageStore = (SimpleMessageStore) other;
            if (!this.fragment.equals(simpleMessageStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Map<GS_VID_T, List<Writable>> tmp;
            tmp = this.messages;
            logger.info(
                    "Before swap. this: "
                            + this.messages.hashCode()
                            + " other: "
                            + simpleMessageStore.messages.hashCode());
            this.messages = simpleMessageStore.messages;
            simpleMessageStore.messages = tmp;
            logger.info(
                    "after swap. this: "
                            + this.messages.hashCode()
                            + " other: "
                            + simpleMessageStore.messages.hashCode());
        } else {
            logger.error("Can not swap with a non-simpleMessageStore obj");
        }
    }

    @Override
    public void clearAll() {
        messages.clear();
    }

    /** Check whether any messages received. */
    @Override
    public boolean anyMessageReceived() {
        return !messages.isEmpty();
    }

    /**
     * Check for lid, any messages available.
     *
     * @param lid lid.
     * @return true if has message
     */
    @Override
    public boolean messageAvailable(long lid) {
        return messages.containsKey(lid);
    }

    @Override
    public Iterable<IN_MSG_T> getMessages(long lid) {
        if (messages.containsKey(lid)) {
            return (Iterable<IN_MSG_T>) messages.get(lid);
        } else {
            // actually a static empty iterator.
            return () -> Collections.emptyIterator();
        }
    }
    /**
     * For a bytestream provided by FFIByteVector, read from it and digest its content.
     *
     * @param vector
     */
    @Override
    public void digest(FFIByteVector vector) {
        throw new IllegalStateException("not implemented");
    }
}

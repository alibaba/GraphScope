package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import io.netty.buffer.ByteBuf;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

/** Specialized store for double msg, use combiner */
public class LongDoubleMessageStoreWithCombiner
        implements MessageStore<LongWritable, DoubleWritable, Long> {

    private static Logger logger = LoggerFactory.getLogger(LongDoubleMessageStore.class);

    private IFragment<?, Long, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<
                    ? extends LongWritable, ? extends Writable, ? extends Writable>
            conf;
    private final MessageCombiner<? super LongWritable, DoubleWritable> messageCombiner;
    private Vertex<Long> vertex;
    /** lid 2 messages */
    //    private Map<Long, List<Double>> messages;
    private Long2DoubleOpenHashMap messages;

    private SingleDoubleWritableIterable iterable;
    private long innerVerticesNum;
    private LongWritable reusableVertexId;
    private DoubleWritable reusableMsgValue;

    public LongDoubleMessageStoreWithCombiner(
            IFragment fragment,
            ImmutableClassesGiraphConfiguration<
                            ? extends LongWritable, ? extends Writable, ? extends Writable>
                    conf,
            MessageCombiner<? super LongWritable, DoubleWritable> messageCombiner) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertex(java.lang.Long.class);
        iterable = new SingleDoubleWritableIterable();
        //        messages = new HashMap<Long,List<Double>>((int) fragment.getInnerVerticesNum());
        messages = new Long2DoubleOpenHashMap((int) fragment.getInnerVerticesNum());
        innerVerticesNum = fragment.getInnerVerticesNum();
        this.messageCombiner = messageCombiner;
        this.reusableMsgValue = new DoubleWritable();
        this.reusableVertexId = new LongWritable();
    }

    @Override
    public void addLidMessage(Long lid, DoubleWritable writable) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }

        if (messages.containsKey(lid)) {
            double preValue = messages.get(lid);
            reusableMsgValue.set(preValue);
            reusableVertexId.set(lid);
            messageCombiner.combine(reusableVertexId, reusableMsgValue, writable);
            messages.put((long) lid, reusableMsgValue.get());
        } else {
            messages.put((long) lid, writable.get());
        }
    }

    @Override
    public void addGidMessages(
            Iterator<Long> gidIterator, Iterator<DoubleWritable> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            long gid = gidIterator.next();
            DoubleWritable msg = writableIterator.next();
            if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
                throw new IllegalStateException("gid to vertex convertion failed: " + gid);
            }
            long lid = vertex.GetValue();
            addLidMessage(lid, msg);
            cnt += 1;
        }
        logger.info("worker [{}] messages to self cnt: {}", fragment.fid(), cnt);
    }

    /**
     * For messages bound with gid, first get lid.
     *
     * @param gid global id
     * @param writable msg
     */
    @Override
    public synchronized void addGidMessage(Long gid, DoubleWritable writable) {
        if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
            throw new IllegalStateException("gid to vertex convertion failed: " + gid);
        }
        long lid = vertex.GetValue();
        addLidMessage(lid, writable);
    }

    /**
     * For input byteBuf, parse and update our store.
     *
     * <p>The received buf contains 4+1+data.
     *
     * @param buf
     */
    public void digestByteBuf(ByteBuf buf) {
        ByteBuf bufCopy = buf.copy();
        bufCopy.skipBytes(5);
        if (bufCopy.readableBytes() % 16 != 0) {
            throw new IllegalStateException("Expect number of bytes times of 16");
        }
        logger.info(
                "LongDoubleMsgStore digest bytebuf size {} direct {}",
                bufCopy.readableBytes(),
                bufCopy.isDirect());
        DoubleWritable tmp = new DoubleWritable();
        while (bufCopy.readableBytes() >= 16) {
            long gid = bufCopy.readLong();
            double msg = bufCopy.readDouble();
            tmp.set(msg);
            addGidMessage(gid, tmp);
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "worker [{}] resolving message to self, gid {}, msg {}",
                        fragment.fid(),
                        gid,
                        msg);
            }
        }
        if (bufCopy.readableBytes() != 0) {
            throw new IllegalStateException("readable bytes no subtracted by 16");
        }
        bufCopy.release();
    }

    @Override
    public void swap(MessageStore<LongWritable, DoubleWritable, Long> other) {
        if (other instanceof LongDoubleMessageStore) {
            LongDoubleMessageStoreWithCombiner longDoubleMessageStore =
                    (LongDoubleMessageStoreWithCombiner) other;
            if (!this.fragment.equals(longDoubleMessageStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Long2DoubleOpenHashMap tmp;
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Before swap {} vs {}", this.messages, longDoubleMessageStore.messages);
            }
            tmp = this.messages;
            this.messages = longDoubleMessageStore.messages;
            longDoubleMessageStore.messages = tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("After swap {} vs {}", this.messages, longDoubleMessageStore.messages);
            }
        } else {
            logger.error("Can not swap with a non-longDoubleMessageStore obj");
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
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        return messages.containsKey(lid);
    }

    /**
     * Avoid creating messages, this function is not thread-safe.
     *
     * @param lid
     * @return
     */
    @Override
    public Iterable<DoubleWritable> getMessages(long lid) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        if (messages.containsKey(lid)) {
            iterable.init(messages.get(lid));
            return iterable;
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

    public static class SingleDoubleWritableIterable implements Iterable<DoubleWritable> {

        private DoubleWritable writable;

        public SingleDoubleWritableIterable() {
            writable = new DoubleWritable();
        }

        public void init(double in) {
            writable.set(in);
        }

        @Override
        public Iterator<DoubleWritable> iterator() {
            return new Iterator<DoubleWritable>() {
                boolean res = true;

                @Override
                public boolean hasNext() {
                    return res;
                }

                @Override
                public DoubleWritable next() {
                    res = false;
                    return writable;
                }
            };
        }
    }
}

package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specialized store for double msg, use combiner
 */
public class LongLongMessageStoreWithCombiner
    implements MessageStore<LongWritable, LongWritable, Long> {

    private static Logger logger = LoggerFactory.getLogger(LongLongMessageStore.class);
    private final MessageCombiner<? super LongWritable, LongWritable> messageCombiner;
    private IFragment<?, Long, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<
        ? extends LongWritable, ? extends Writable, ? extends Writable>
        conf;
    private Vertex<Long> vertex;
    /**
     * lid 2 messages
     */
    //    private Map<Long, List<Double>> messages;
    private Long2LongOpenHashMap messages;

    private SingleLongWritableIterable iterable;
    private long innerVerticesNum;
    private LongWritable reusableVertexId;
    private LongWritable reusableMsgValue;

    public LongLongMessageStoreWithCombiner(
        IFragment fragment,
        ImmutableClassesGiraphConfiguration<
            ? extends LongWritable, ? extends Writable, ? extends Writable>
            conf,
        MessageCombiner<? super LongWritable, LongWritable> messageCombiner) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertex(java.lang.Long.class);
        iterable = new SingleLongWritableIterable();
        //        messages = new HashMap<Long,List<Double>>((int) fragment.getInnerVerticesNum());
        messages = new Long2LongOpenHashMap((int) fragment.getInnerVerticesNum());
        innerVerticesNum = fragment.getInnerVerticesNum();
        this.messageCombiner = messageCombiner;
        this.reusableMsgValue = new LongWritable();
        this.reusableVertexId = new LongWritable();
    }

    @Override
    public void addLidMessage(Long lid, LongWritable writable) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }

        addLidMessage0(lid, writable.get());
    }

    public void addLidMessage0(Long lid, long msg){
        if (messages.containsKey(lid)) {
            long preValue = messages.get(lid);
            messages.put((long) lid, Math.min(msg, messages.get(lid)));
        } else {
            messages.put((long) lid, msg);
        }
    }

    @Override
    public void addGidMessages(
        Iterator<Long> gidIterator, Iterator<LongWritable> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            long gid = gidIterator.next();
            LongWritable msg = writableIterator.next();
            if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
                throw new IllegalStateException("gid to vertex convertion failed: " + gid);
            }
            long lid = vertex.GetValue();
            addLidMessage0(lid, msg.get());
            cnt += 1;
        }
        logger.info("worker [{}] messages to self cnt: {}", fragment.fid(), cnt);
    }

    /**
     * For messages bound with gid, first get lid.
     *
     * @param gid      global id
     * @param writable msg
     */
    @Override
    public synchronized void addGidMessage(Long gid, LongWritable writable) {
        addGidMessage0((Long) gid, writable.get());
    }

    private synchronized void addGidMessage0(Long gid, long msg) {
        if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
            throw new IllegalStateException("gid to vertex convertion failed: " + gid);
        }
        long lid = vertex.GetValue();
        addLidMessage0(lid, msg);
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
            "LongLongMsgStore digest bytebuf size {} direct {}",
            bufCopy.readableBytes(),
            bufCopy.isDirect());
        LongWritable tmp = new LongWritable();
        while (bufCopy.readableBytes() >= 16) {
            long gid = bufCopy.readLong();
            long msg = bufCopy.readLong();
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
    public void swap(MessageStore<LongWritable, LongWritable, Long> other) {
        if (other instanceof LongLongMessageStoreWithCombiner) {
            LongLongMessageStoreWithCombiner other1 =
                (LongLongMessageStoreWithCombiner) other;
            if (!this.fragment.equals(other1.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Long2LongOpenHashMap tmp;
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Before swap {} vs {}", this.messages, other1.messages);
            }
            tmp = this.messages;
            this.messages = other1.messages;
            other1.messages = tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("After swap {} vs {}", this.messages, other1.messages);
            }
        } else {
            logger.error("Can not swap with a non-LongLongMessageStore obj");
        }
    }

    @Override
    public void clearAll() {
        messages.clear();
    }

    /**
     * Check whether any messages received.
     */
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
    public Iterable<LongWritable> getMessages(long lid) {
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
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            return;
        }
        if (size % 16 != 8) {
            throw new IllegalStateException("Expect number of bytes times of 16 + 8, actual: " + size);
        }
        try {
            long dataSize = inputStream.readLong();
            if (dataSize != size - 8){
                throw new IllegalStateException("Message size not match: vec size" + size + ", first field in vec" + dataSize);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.debug("LongDoubleMsgStore digest FFIVector size {}", size);
        try {
            while (inputStream.longAvailable() >= 16) {
                long gid = inputStream.readLong();
                long msg = inputStream.readLong();
                addGidMessage0(gid, msg);
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "worker [{}] resolving message to self, gid {}, msg {}",
                        fragment.fid(),
                        gid,
                        msg);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputStream.longAvailable() != 0) {
            throw new IllegalStateException("readable bytes no subtracted by 16");
        }
    }

    public static class SingleLongWritableIterable implements Iterable<LongWritable> {

        private LongWritable writable;

        public SingleLongWritableIterable() {
            writable = new LongWritable();
        }

        public void init(long in) {
            writable.set(in);
        }

        @Override
        public Iterator<LongWritable> iterator() {
            return new Iterator<LongWritable>() {
                boolean res = true;

                @Override
                public boolean hasNext() {
                    return res;
                }

                @Override
                public LongWritable next() {
                    res = false;
                    return writable;
                }
            };
        }
    }
}

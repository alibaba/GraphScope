package com.alibaba.graphscope.parallel.cache;

import com.alibaba.graphscope.parallel.cache.impl.BatchWritableMessageCache;
import com.alibaba.graphscope.parallel.cache.impl.ByteBufMessageCache;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message cache is a special case of SendDataCache, in which the stored data is a pair (I,D),I is
 * the vertex OID, D is the message for Vertex.
 */
public interface SendMessageCache<I extends WritableComparable,
    M extends Writable, GS_VID_T> {
    Logger logger = LoggerFactory.getLogger(SendMessageCache.class);

    void sendMessage(int dstFragId, GS_VID_T gid, M message);

    void removeMessageToSelf(MessageStore<I, M, GS_VID_T> nextIncomingMessages);

    /**
     * FLush all cached messages out.
     */
    void flushMessage();

    void clear();


    static <I_ extends WritableComparable,M_ extends Writable,GS_VID_T_>SendMessageCache<I_, M_, GS_VID_T_> newMessageCache(int fragNum, int fragId,
        NettyClient client, ImmutableClassesGiraphConfiguration<I_, ?, M_> conf) {
        String outMsgCacheType = conf.getOutMessageCacheType();
        logger.info("Creating Out Message cache of type [{}]", outMsgCacheType);
        if (outMsgCacheType.equals("BatchWritable")){
            return new BatchWritableMessageCache<>(fragNum,fragId, client, conf);
        }
        else if (outMsgCacheType.equals("ByteBuf")){
            return new ByteBufMessageCache<>(fragNum, fragId, client, conf);
        }
        else {
            throw new IllegalStateException("BatchWritable or ByteBuf");
        }
    }
}

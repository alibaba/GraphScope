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
public interface SendMessageCache<I extends WritableComparable, M extends Writable, GS_VID_T> {

    Logger logger = LoggerFactory.getLogger(SendMessageCache.class);

    static <I_ extends WritableComparable, M_ extends Writable, GS_VID_T_>
            SendMessageCache<I_, M_, GS_VID_T_> newMessageCache(
                    int fragNum,
                    int fragId,
                    NettyClient client,
                    ImmutableClassesGiraphConfiguration<I_, ?, M_> conf) {
        String outMsgCacheType = conf.getOutMessageCacheType();
        logger.info("Creating Out Message cache of type [{}]", outMsgCacheType);
        if (outMsgCacheType.equals("BatchWritable")) {
            return new BatchWritableMessageCache<>(fragNum, fragId, client, conf);
        } else if (outMsgCacheType.equals("ByteBuf")) {
            return new ByteBufMessageCache<>(fragNum, fragId, client, conf);
        } else {
            throw new IllegalStateException("BatchWritable or ByteBuf");
        }
    }

    void sendMessage(int dstFragId, GS_VID_T gid, M message);

    void removeMessageToSelf(MessageStore<I, M, GS_VID_T> nextIncomingMessages);

    /**
     * FLush all cached messages out.
     */
    void flushMessage();

    void clear();
}

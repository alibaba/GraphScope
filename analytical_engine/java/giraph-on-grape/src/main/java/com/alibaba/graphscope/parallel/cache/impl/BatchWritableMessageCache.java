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
package com.alibaba.graphscope.parallel.cache.impl;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_AGGREGATE_SIZE;

import com.alibaba.graphscope.parallel.cache.SendMessageCache;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.NettyClient;
import com.alibaba.graphscope.parallel.netty.request.impl.BatchWritableRequest;
import com.alibaba.graphscope.utils.Gid2Data;
import com.alibaba.graphscope.utils.Gid2DataFixed;
import com.alibaba.graphscope.utils.Gid2DataResizable;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

public class BatchWritableMessageCache<I extends WritableComparable, M extends Writable, GS_VID_T>
        implements SendMessageCache<I, M, GS_VID_T> {

    private static Logger logger = LoggerFactory.getLogger(BatchWritableMessageCache.class);

    private final int fragNum;
    private final int fragId;
    private final NettyClient client;
    private final ImmutableClassesGiraphConfiguration<I, ?, ?> conf;
    /**
     * cacheSize.
     */
    private int cacheSize;

    private Gid2Data[] cache;

    public BatchWritableMessageCache(
            int fragNum,
            int fragId,
            NettyClient client,
            ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
        this.fragNum = fragNum;
        this.fragId = fragId;
        this.client = client;
        this.conf = conf;

        cacheSize = MESSAGE_AGGREGATE_SIZE.get(conf);
        logger.info("Using message aggregate size: " + cacheSize);

        cache = new Gid2Data[fragNum];
        for (int i = 0; i < fragNum; ++i) {
            if (i == fragId) {
                // Message to self can be resiable.
                cache[i] = Gid2Data.newResizable(cacheSize);
            } else {
                cache[i] = Gid2Data.newFixed(cacheSize);
            }
        }
    }

    @Override
    public void sendMessage(int dstFragId, GS_VID_T gid, M message) {
        // TODO: GS_VID_T can be types other than long.
        if (!cache[dstFragId].add((Long) gid, message)) {
            if (dstFragId == fragId) {
                throw new IllegalStateException("message to self can not be failed");
            }
            // If add cache fail, then cache is already full.
            BatchWritableRequest request =
                    new BatchWritableRequest((Gid2DataFixed) cache[dstFragId]);
            logger.info(
                    "frag ["
                            + fragId
                            + "] msg to ["
                            + dstFragId
                            + "] "
                            + cache[dstFragId].size()
                            + " full, flush and sending");
            client.sendMessage(dstFragId, request);
            //            cache[dstFragId].clear();
            // If we clear the original gid2dataFixed, it seems to affect the message sent.
            cache[dstFragId] = Gid2Data.newFixed(cacheSize);
            // resend
            cache[dstFragId].add((Long) gid, message);
        } else {
            // if add success, invoke log.
            //            logger.debug("frag [" + fragId + "] Send msg to [" + dstFragId + "], gid:"
            // + gid + ", msg:" + message);
        }
    }

    @Override
    public void removeMessageToSelf(MessageStore<I, M, GS_VID_T> nextIncomingMessages) {
        if (Objects.nonNull(cache[fragId])) {
            Gid2DataResizable messageToSelf = (Gid2DataResizable) cache[fragId];
            ArrayList<Long> gids = messageToSelf.getGids();
            ArrayList<Writable> msgs = messageToSelf.getData();
            logger.info(
                    "worker: ["
                            + fragId
                            + "] messages to self should be "
                            + gids.size()
                            + "=="
                            + msgs.size());
            //            for (int i = 0; i < messageToSelf.size(); ++i){
            nextIncomingMessages.addGidMessages(
                    (Iterator<GS_VID_T>) gids.iterator(), (Iterator<M>) msgs.iterator());
            //            }
        }
    }

    /**
     * FLush all cached messages out. after flush, clear should be called.
     */
    @Override
    public void flushMessage() {
        sendCurrentMessageInCache();
        client.waitAllRequests();
    }

    @Override
    public void clear() {
        for (int i = 0; i < cache.length; ++i) {
            cache[i].clear();
        }
    }

    /**
     * After the execution, we need to flush all message still in cache.
     */
    private void sendCurrentMessageInCache() {
        for (int dstFragId = 0; dstFragId < cache.length; ++dstFragId) {
            if (dstFragId != fragId && cache[dstFragId].size() > 0) {
                BatchWritableRequest request =
                        new BatchWritableRequest((Gid2DataFixed) cache[dstFragId]);
                logger.info(
                        "frag ["
                                + fragId
                                + "] msg to ["
                                + dstFragId
                                + "], size: "
                                + cache[dstFragId].size()
                                + " flush and sending");
                client.sendMessage(dstFragId, request);
            }
        }
        logger.info("frag [" + fragId + "] finish flushing cache");
    }
}

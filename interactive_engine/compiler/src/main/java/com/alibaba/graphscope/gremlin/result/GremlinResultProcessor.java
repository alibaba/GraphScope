/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.common.client.ResultParser;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import io.grpc.Status;
import io.netty.channel.ChannelHandlerContext;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GremlinResultProcessor extends StandardOpProcessor implements ResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(GremlinResultProcessor.class);
    protected Context writeResult;
    protected List<Object> resultCollectors = new ArrayList<>();
    protected boolean locked = false;
    protected ResultParser resultParser;

    public GremlinResultProcessor(Context writeResult, ResultParser resultParser) {
        this.writeResult = writeResult;
        this.resultParser = resultParser;
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                if (!locked) {
                    resultCollectors.addAll(resultParser.parseFrom(response));
                }
            } catch (Exception e) {
                writeResultList(
                        writeResult,
                        Collections.singletonList(e.getMessage()),
                        ResponseStatusCode.SERVER_ERROR);
                // cannot write to this context any more
                locked = true;
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void finish() {
        synchronized (this) {
            if (!locked) {
                formatResultIfNeed();
                writeResultList(writeResult, resultCollectors, ResponseStatusCode.SUCCESS);
                locked = true;
            }
        }
    }

    // format group result as a single map
    protected void formatResultIfNeed() {
        if (resultParser instanceof GroupResultParser) {
            Map groupResult = new LinkedHashMap();
            resultCollectors.forEach(
                    k -> {
                        groupResult.putAll((Map) k);
                    });
            resultCollectors.clear();
            resultCollectors.add(groupResult);
        }
    }

    @Override
    public void error(Status status) {
        synchronized (this) {
            if (!locked) {
                writeResultList(
                        writeResult,
                        Collections.singletonList(status.toString()),
                        ResponseStatusCode.SERVER_ERROR);
                locked = true;
            }
        }
    }

    protected void writeResultList(
            final Context context,
            final List<Object> resultList,
            final ResponseStatusCode statusCode) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();

        if (statusCode == ResponseStatusCode.SERVER_ERROR) {
            ResponseMessage.Builder builder =
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR);
            if (resultList.size() > 0) {
                builder.statusMessage((String) resultList.get(0));
            }
            ctx.writeAndFlush(builder.create());
            return;
        }

        boolean retryOnce = false;
        while (true) {
            if (ctx.channel().isWritable()) {
                Frame frame = null;
                try {
                    frame =
                            makeFrame(
                                    context,
                                    msg,
                                    serializer,
                                    useBinary,
                                    resultList,
                                    statusCode,
                                    Collections.emptyMap(),
                                    Collections.emptyMap());
                    ctx.writeAndFlush(frame).get();
                    break;
                } catch (Exception e) {
                    if (frame != null) {
                        frame.tryRelease();
                    }
                    logger.error(
                            "write "
                                    + resultList.size()
                                    + " result to context "
                                    + context
                                    + " status code=>"
                                    + statusCode
                                    + " fail",
                            e);
                    throw new RuntimeException(e);
                }
            } else {
                if (retryOnce) {
                    String message =
                            "write result to context fail for context " + msg + " is too busy";
                    logger.error(message);
                    throw new RuntimeException(message);
                } else {
                    logger.warn(
                            "Pausing response writing as writeBufferHighWaterMark exceeded on "
                                    + msg
                                    + " - writing will continue once client has caught up");
                    retryOnce = true;
                    try {
                        TimeUnit.MILLISECONDS.sleep(10L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}

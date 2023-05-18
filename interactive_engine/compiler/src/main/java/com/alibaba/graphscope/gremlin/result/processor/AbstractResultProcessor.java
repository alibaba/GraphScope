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

package com.alibaba.graphscope.gremlin.result.processor;

import com.alibaba.graphscope.common.result.ResultParser;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import io.grpc.Status;
import io.netty.channel.ChannelHandlerContext;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class AbstractResultProcessor extends StandardOpProcessor
        implements ResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(AbstractResultProcessor.class);

    protected final Context writeResult;
    protected final ResultParser resultParser;

    protected final List<Object> resultCollectors;
    protected final int resultCollectorsBatchSize;

    protected boolean locked;

    protected AbstractResultProcessor(Context writeResult, ResultParser resultParser) {
        this.writeResult = writeResult;
        this.resultParser = resultParser;
        RequestMessage msg = writeResult.getRequestMessage();
        Settings settings = writeResult.getSettings();
        // init batch size from resultIterationBatchSize in conf/gremlin-server.yaml,
        // or args in RequestMessage which is originate from gremlin client
        this.resultCollectorsBatchSize =
                (Integer)
                        msg.optionalArgs(Tokens.ARGS_BATCH_SIZE)
                                .orElse(settings.resultIterationBatchSize);
        this.resultCollectors = new ArrayList<>(this.resultCollectorsBatchSize);
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                if (!locked) {
                    // send back a page of results if batch size is met and then reset the
                    // resultCollectors
                    if (this.resultCollectors.size() >= this.resultCollectorsBatchSize) {
                        aggregateResults();
                        writeResultList(
                                writeResult, resultCollectors, ResponseStatusCode.PARTIAL_CONTENT);
                        this.resultCollectors.clear();
                    }
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
                aggregateResults();
                writeResultList(writeResult, resultCollectors, ResponseStatusCode.SUCCESS);
                locked = true;
            }
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

    protected abstract void aggregateResults();

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

/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.gremlin.plugin.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtils;
import org.javatuples.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintain the gremlin execution context for http request.
 */
public class HttpContext extends Context {
    private final Pair<String, MessageTextSerializer<?>> serializer;
    private final boolean keepAlive;
    private final AtomicReference<Boolean> finalResponse;

    public HttpContext(
            RequestMessage requestMessage,
            ChannelHandlerContext ctx,
            Settings settings,
            GraphManager graphManager,
            GremlinExecutor gremlinExecutor,
            ScheduledExecutorService scheduledExecutorService,
            Pair<String, MessageTextSerializer<?>> serializer,
            boolean keepAlive) {
        super(
                requestMessage,
                ctx,
                settings,
                graphManager,
                gremlinExecutor,
                scheduledExecutorService);
        this.serializer = Objects.requireNonNull(serializer);
        this.keepAlive = keepAlive;
        this.finalResponse = new AtomicReference<>(false);
    }

    /**
     * serialize the response message to http response and write to http channel.
     * @param responseMessage
     */
    @Override
    public void writeAndFlush(final ResponseMessage responseMessage) {
        try {
            if (finalResponse.compareAndSet(
                    false, responseMessage.getStatus().getCode().isFinalResponse())) {
                if (responseMessage.getStatus().getCode() == ResponseStatusCode.SUCCESS) {
                    Object data = responseMessage.getResult().getData();
                    if (!keepAlive && ObjectUtils.isEmpty(data)) {
                        this.getChannelHandlerContext().close();
                        return;
                    }
                }
                ByteBuf byteBuf =
                        Unpooled.wrappedBuffer(
                                serializer
                                        .getValue1()
                                        .serializeResponseAsString(responseMessage)
                                        .getBytes(StandardCharsets.UTF_8));
                FullHttpResponse response =
                        new DefaultFullHttpResponse(
                                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
                ChannelFuture channelFuture =
                        this.getChannelHandlerContext().writeAndFlush(response);
                ResponseStatusCode statusCode = responseMessage.getStatus().getCode();
                if (!keepAlive && statusCode.isFinalResponse()) {
                    channelFuture.addListener(ChannelFutureListener.CLOSE);
                }
            }
        } catch (Exception e) {
            HttpHandlerUtils.sendError(
                    this.getChannelHandlerContext(),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage(),
                    keepAlive);
        }
    }
}

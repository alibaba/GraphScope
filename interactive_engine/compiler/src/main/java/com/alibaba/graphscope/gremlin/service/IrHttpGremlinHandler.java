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

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.plugin.processor.HttpContext;
import com.alibaba.graphscope.gremlin.plugin.processor.IrOpLoader;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.*;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtils;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IrHttpGremlinHandler extends HttpGremlinEndpointHandler {
    private static final Logger logger = LoggerFactory.getLogger(HttpGremlinEndpointHandler.class);
    private final Map<String, MessageSerializer<?>> serializers;
    private final GremlinExecutor gremlinExecutor;
    private final GraphManager graphManager;
    private final Settings settings;
    private final Pattern pattern;

    public IrHttpGremlinHandler(
            final Map<String, MessageSerializer<?>> serializers,
            final GremlinExecutor gremlinExecutor,
            final GraphManager graphManager,
            final Settings settings) {
        super(serializers, gremlinExecutor, graphManager, settings);
        this.serializers =
                Utils.getFieldValue(HttpGremlinEndpointHandler.class, this, "serializers");
        this.gremlinExecutor =
                Utils.getFieldValue(HttpGremlinEndpointHandler.class, this, "gremlinExecutor");
        this.graphManager =
                Utils.getFieldValue(HttpGremlinEndpointHandler.class, this, "graphManager");
        this.settings = Utils.getFieldValue(HttpGremlinEndpointHandler.class, this, "settings");
        this.pattern = Pattern.compile("(.*);q=(.*)");
    }

    /**
     * Convert {@code FullHttpRequest} to {@code RequestMessage}, and process the request by {@code IrStandardOpProcessor}
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest req = (FullHttpRequest) msg;
            boolean keepAlive = HttpUtil.isKeepAlive(req);
            try {
                if ("/favicon.ico".equals(req.uri())) {
                    HttpHandlerUtils.sendError(
                            ctx,
                            HttpResponseStatus.NOT_FOUND,
                            "Gremlin Server doesn't have a favicon.ico",
                            keepAlive);
                    return;
                }

                if (HttpUtil.is100ContinueExpected(req)) {
                    ctx.write(
                            new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
                }

                if (req.method() != HttpMethod.GET && req.method() != HttpMethod.POST) {
                    HttpHandlerUtils.sendError(
                            ctx,
                            HttpResponseStatus.METHOD_NOT_ALLOWED,
                            HttpResponseStatus.METHOD_NOT_ALLOWED.toString(),
                            keepAlive);
                    return;
                }

                RequestMessage requestMessage;
                try {
                    requestMessage = createRequestMessage(req);
                } catch (Exception e) {
                    HttpHandlerUtils.sendError(
                            ctx, HttpResponseStatus.BAD_REQUEST, e.getMessage(), keepAlive);
                    return;
                }

                String acceptString =
                        Optional.ofNullable(req.headers().get("Accept")).orElse("application/json");
                Pair<String, MessageTextSerializer<?>> serializer =
                        this.chooseSerializer(acceptString);
                if (null == serializer) {
                    HttpHandlerUtils.sendError(
                            ctx,
                            HttpResponseStatus.BAD_REQUEST,
                            String.format(
                                    "no serializer for requested Accept header: %s", acceptString),
                            keepAlive);
                    return;
                }

                HttpContext context =
                        new HttpContext(
                                requestMessage,
                                ctx,
                                this.settings,
                                this.graphManager,
                                this.gremlinExecutor,
                                this.gremlinExecutor.getScheduledExecutorService(),
                                serializer,
                                keepAlive);

                OpProcessor opProcessor = IrOpLoader.getProcessor("").get();
                opProcessor.select(context).accept(context);
            } catch (Exception e) {
                Throwable t = ExceptionUtils.getRootCause(e);
                if (t instanceof TooLongFrameException) {
                    HttpHandlerUtils.sendError(
                            ctx,
                            HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                            t.getMessage() + " - increase the maxContentLength",
                            false);
                } else {
                    String message = (t != null) ? t.getMessage() : e.getMessage();
                    HttpHandlerUtils.sendError(
                            ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, message, keepAlive);
                }
            } finally {
                if (req.refCnt() > 0) {
                    boolean fullyRelease = req.release();
                    if (!fullyRelease) {
                        logger.warn(
                                "http request message was not fully released, may cause a"
                                        + " memory leak");
                    }
                }
            }
        }
    }

    private Pair<String, MessageTextSerializer<?>> chooseSerializer(final String acceptString) {
        List<Pair<String, Double>> ordered =
                Stream.of(acceptString.split(","))
                        .map(
                                (mediaType) -> {
                                    Matcher matcher = pattern.matcher(mediaType);
                                    return matcher.matches()
                                            ? Pair.with(
                                                    matcher.group(1),
                                                    Double.parseDouble(matcher.group(2)))
                                            : Pair.with(mediaType, 1.0);
                                })
                        .sorted(
                                (o1, o2) -> {
                                    return ((String) o2.getValue0())
                                            .compareTo((String) o1.getValue0());
                                })
                        .collect(Collectors.toList());
        Iterator var3 = ordered.iterator();

        String accept;
        do {
            if (!var3.hasNext()) {
                return null;
            }

            Pair<String, Double> p = (Pair) var3.next();
            accept = p.getValue0().equals("*/*") ? "application/json" : p.getValue0();
        } while (!this.serializers.containsKey(accept));

        return Pair.with(accept, (MessageTextSerializer) this.serializers.get(accept));
    }

    private RequestMessage createRequestMessage(FullHttpRequest httpRequest) {
        Quartet<String, Map<String, Object>, String, Map<String, String>> req =
                HttpHandlerUtils.getRequestArguments(httpRequest);
        return RequestMessage.build("eval")
                .addArg("gremlin", req.getValue0())
                .addArg("bindings", req.getValue1())
                .addArg("language", req.getValue2())
                .addArg("aliases", req.getValue3())
                .create();
    }
}

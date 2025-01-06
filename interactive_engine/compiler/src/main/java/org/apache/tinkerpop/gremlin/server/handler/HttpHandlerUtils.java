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

package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.javatuples.Quartet;

import java.util.Map;
import java.util.Optional;

public class HttpHandlerUtils {
    public static final Quartet<String, Map<String, Object>, String, Map<String, String>>
            getRequestArguments(final FullHttpRequest request) {
        return HttpHandlerUtil.getRequestArguments(request);
    }

    public static final void sendError(
            final ChannelHandlerContext ctx,
            final HttpResponseStatus status,
            final String message,
            final boolean keepAlive) {
        sendError(ctx, status, message, Optional.empty(), keepAlive);
    }

    public static final void sendError(
            final ChannelHandlerContext ctx,
            final HttpResponseStatus status,
            final String message,
            final Optional<Throwable> t,
            final boolean keepAlive) {
        HttpHandlerUtil.sendError(ctx, status, message, t, keepAlive);
    }
}

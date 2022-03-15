/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.utils;

import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import org.apache.giraph.comm.requests.NettyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * RequestUtils utility class
 */
public class RequestUtils {

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(RequestUtils.class);

    /**
     * Private Constructor
     */
    private RequestUtils() {}

    /**
     * decodeWritableRequest based on predicate
     *
     * @param buf     ByteBuf
     * @param request writableRequest
     * @return properly initialized writableRequest
     * @throws IOException
     */
    public static NettyMessage decodeNettyMessage(ByteBuf buf, NettyMessage request)
            throws IOException {
        ByteBufInputStream input = new ByteBufInputStream(buf);
        request.readFields(input);
        return request;
    }

    /**
     * decodeWritableRequest based on predicate
     *
     * @param buf     ByteBuf
     * @param request writableRequest
     * @return properly initialized writableRequest
     * @throws IOException
     */
    public static WritableRequest decodeWritableRequest(ByteBuf buf, WritableRequest request)
            throws IOException {
        if (request.getRequestType().getClazz().equals(ByteBufRequest.class)) {
            request.setBuffer(buf);
            return request;
        }
        ByteBufInputStream input = new ByteBufInputStream(buf);
        request.readFields(input);
        return request;
    }
}

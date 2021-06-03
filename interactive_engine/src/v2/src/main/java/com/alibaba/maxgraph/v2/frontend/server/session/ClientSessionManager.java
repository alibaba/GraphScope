/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.server.session;

import org.apache.tinkerpop.gremlin.server.Context;

import java.util.UUID;

/**
 * manager request id and session id from gremlin client
 */
public class ClientSessionManager implements SessionManager {
    private Context context;

    public ClientSessionManager(Context context) {
        this.context = context;
    }

    @Override
    public UUID getRequestId() {
        return this.context.getRequestMessage().getRequestId();
    }

    @Override
    public String getSessionId() {
        return this.context.getChannelHandlerContext().channel().id().asLongText();
    }
}

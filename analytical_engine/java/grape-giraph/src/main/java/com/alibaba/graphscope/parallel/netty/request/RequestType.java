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
package com.alibaba.graphscope.parallel.netty.request;

import com.alibaba.graphscope.parallel.netty.request.impl.BatchWritableRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.GidLongWritableRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.OneLongWritableRequest;

public enum RequestType {
    ONE_LONG_WRITABLE_REQUEST(OneLongWritableRequest.class),
    GID_Long_WRITABLE_REQUEST(GidLongWritableRequest.class),
    BATCH_WRITABLE_REQUEST(BatchWritableRequest.class),
    BYTEBUF_REQUEST(ByteBufRequest.class);

    private Class<? extends WritableRequest> clz;

    RequestType(Class<? extends WritableRequest> clz) {
        this.clz = clz;
    }

    public Class<? extends WritableRequest> getClazz() {
        return clz;
    }
}

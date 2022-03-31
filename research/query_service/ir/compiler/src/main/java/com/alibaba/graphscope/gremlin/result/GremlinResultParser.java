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
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Collections;
import java.util.List;

public interface GremlinResultParser extends ResultParser {
    @Override
    default List<Object> parseFrom(PegasusClient.JobResponse response) {
        try {
            IrResult.Results results = IrResult.Results.parseFrom(response.getData());
            Object parseResult = parseFrom(results);
            if (parseResult instanceof EmptyValue) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(parseResult);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new GremlinResultParserException("parse from proto failed " + e);
        }
    }

    Object parseFrom(IrResult.Results results);
}

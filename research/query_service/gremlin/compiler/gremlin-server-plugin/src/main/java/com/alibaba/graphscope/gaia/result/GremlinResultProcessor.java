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
package com.alibaba.graphscope.gaia.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.alibaba.graphscope.gaia.processor.GaiaGraphOpProcessor;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GremlinResultProcessor implements ResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(GremlinResultProcessor.class);
    private Context writeResult;
    private List<Object> resultCollectors = new ArrayList<>();
    private boolean locked = false;
    private ResultParser resultParser;

    public GremlinResultProcessor(Context writeResult, ResultParser resultParser) {
        this.writeResult = writeResult;
        this.resultParser = resultParser;
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                if (!locked) {
                    logger.debug("start to process response");
                    GremlinResult.Result resultData = GremlinResult.Result.parseFrom(response.getData());
                    if (resultData.toByteString().equals(ByteString.EMPTY)) {
                        logger.error("data is empty");
                    }
                    logger.debug("data is {}", resultData);
                    resultCollectors.addAll(resultParser.parseFrom(resultData));
                }
            } catch (Exception e) {
                GaiaGraphOpProcessor.writeResultList(writeResult, Collections.singletonList(e.getMessage()), ResponseStatusCode.SERVER_ERROR);
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
                logger.debug("start finish");
                GaiaGraphOpProcessor.writeResultList(writeResult, resultCollectors, ResponseStatusCode.SUCCESS);
                locked = true;
            }
        }
    }

    @Override
    public void error(Status status) {
        synchronized (this) {
            if (!locked) {
                logger.debug("start error");
                GaiaGraphOpProcessor.writeResultList(writeResult, Collections.singletonList(status.toString()), ResponseStatusCode.SERVER_ERROR);
                locked = true;
            }
        }
    }
}

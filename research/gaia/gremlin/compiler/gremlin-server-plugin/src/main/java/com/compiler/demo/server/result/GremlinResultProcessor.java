/**
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
package com.compiler.demo.server.result;

import com.alibaba.graphscope.common.proto.GremlinResult;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.proto.PegasusClient;
import com.compiler.demo.server.MaxGraphOpProcessor;
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

    public GremlinResultProcessor(Context writeResult) {
        this.writeResult = writeResult;
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                if (!locked) {
                    logger.info("start to process response {}", GremlinResult.Result.parseFrom(response.getData()));
                    if (response.getResultCase() == PegasusClient.JobResponse.ResultCase.DATA) {
                        resultCollectors.addAll(ResultParser.parseFrom(response));
                    }
                }
            } catch (Exception e) {
                MaxGraphOpProcessor.writeResultList(writeResult, Collections.singletonList(e.getMessage()), ResponseStatusCode.SERVER_ERROR);
                // cannot write to this context any more
                locked = true;
            }
        }
    }

    @Override
    public void finish() {
        synchronized (this) {
            if (!locked) {
                logger.info("start to process finish");
                MaxGraphOpProcessor.writeResultList(writeResult, resultCollectors, ResponseStatusCode.SUCCESS);
                locked = true;
            }
        }
    }

    @Override
    public void error(Status status) {
        synchronized (this) {
            if (!locked) {
                logger.info("start to process error");
                MaxGraphOpProcessor.writeResultList(writeResult, Collections.singletonList(status.toString()), ResponseStatusCode.SERVER_ERROR);
                locked = true;
            }
        }
    }
}

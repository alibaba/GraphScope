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

import java.util.Collections;

public class GremlinResultProcessor implements ResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(GremlinResultProcessor.class);
    private Context writeResult;
    private boolean hasResult = false;

    public GremlinResultProcessor(Context writeResult) {
        this.writeResult = writeResult;
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                logger.info("start to process response {}", GremlinResult.Result.parseFrom(response.getData()));
                if (!hasResult) {
                    if (response.getResultCase() == PegasusClient.JobResponse.ResultCase.ERR) {
                        MaxGraphOpProcessor.writeResultList(writeResult, Collections.EMPTY_LIST, ResponseStatusCode.SERVER_ERROR);
                    } else {
                        MaxGraphOpProcessor.writeResultList(writeResult, ResultParser.parseFrom(response), ResponseStatusCode.SUCCESS);
                    }
                }
            } catch (Exception e) {
                logger.error("exception is {}", e);
                MaxGraphOpProcessor.writeResultList(writeResult, Collections.EMPTY_LIST, ResponseStatusCode.SERVER_ERROR);
            } finally {
                hasResult = true;
            }
        }
    }

    @Override
    public void finish() {
        synchronized (this) {
            if (!hasResult) {
                logger.info("start to process finish");
                MaxGraphOpProcessor.writeResultList(writeResult, Collections.EMPTY_LIST, ResponseStatusCode.SUCCESS);
                hasResult = true;
            }
        }
    }

    @Override
    public void error(Status status) {
        synchronized (this) {
            if (!hasResult) {
                logger.info("start to process error");
                MaxGraphOpProcessor.writeResultList(writeResult, Collections.EMPTY_LIST, ResponseStatusCode.SERVER_ERROR);
                hasResult = true;
            }
        }
    }
}

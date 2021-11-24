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

package com.alibaba.graphscope.common.client;

import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IrResultProcessor implements ResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(IrResultProcessor.class);
    private List<Object> resultCollectors = new ArrayList<>();
    private boolean locked = false;
    private ResultParser resultParser;

    public IrResultProcessor(ResultParser resultParser) {
        this.resultParser = resultParser;
    }

    @Override
    public void process(PegasusClient.JobResponse response) {
        synchronized (this) {
            try {
                if (!locked) {
                    resultCollectors.addAll(resultParser.parseFrom(response));
                }
            } catch (Exception e) {
                logger.error("process fail {}", e.getMessage());
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
                locked = true;
            }
        }
    }

    @Override
    public void error(Status status) {
        synchronized (this) {
            if (!locked) {
                logger.error("status error {}", status);
                locked = true;
            }
        }
    }
}

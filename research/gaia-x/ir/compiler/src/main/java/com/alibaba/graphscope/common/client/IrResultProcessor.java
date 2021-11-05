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

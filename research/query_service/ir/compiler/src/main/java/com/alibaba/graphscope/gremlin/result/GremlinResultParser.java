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

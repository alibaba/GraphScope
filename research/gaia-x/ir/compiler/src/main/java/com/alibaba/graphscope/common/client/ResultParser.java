package com.alibaba.graphscope.common.client;

import com.alibaba.pegasus.service.protocol.PegasusClient;

import java.util.List;

public interface ResultParser {
    List<Object> parseFrom(PegasusClient.JobResponse response);
}

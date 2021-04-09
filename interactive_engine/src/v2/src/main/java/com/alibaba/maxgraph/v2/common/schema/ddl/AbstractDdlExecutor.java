package com.alibaba.maxgraph.v2.common.schema.ddl;

import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public abstract class AbstractDdlExecutor {

    public abstract DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException;

}

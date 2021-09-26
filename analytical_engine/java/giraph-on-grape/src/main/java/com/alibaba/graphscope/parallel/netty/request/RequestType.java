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

    RequestType(Class<? extends WritableRequest> clz){
        this.clz = clz;
    }

    public Class<? extends WritableRequest> getClazz(){
        return clz;
    }

}

package org.apache.giraph.comm.requests;

public enum NettyMessageType {
    BYTE_ARRAY_MESSAGE(ByteArrayMessage.class),
    AGGREGATOR_MESSAGE(AggregatorMessage.class),
    NETTY_WRITABLE_MESSAGE(NettyWritableMessage.class);

    private Class<? extends NettyMessage> requestClass;

    private NettyMessageType(Class<? extends NettyMessage> clz) {
        this.requestClass = clz;
    }

    public Class<? extends NettyMessage> getRequestClass() {
        return requestClass;
    }
}

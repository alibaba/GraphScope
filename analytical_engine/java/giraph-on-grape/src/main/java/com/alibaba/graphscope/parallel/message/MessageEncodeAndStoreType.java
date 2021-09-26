package com.alibaba.graphscope.parallel.message;


/**
 * There are two types of message-stores currently
 * pointer based, and default byte-array based
 */
public enum MessageEncodeAndStoreType {
    SIMPLE_MESSAGE_STORE(false);

    /** Can use one message to many ids encoding? */
    private final boolean oneMessageToManyIdsEncoding;

    /**
     * Constructor
     *
     * @param oneMessageToManyIdsEncoding use one message to many ids encoding
     */
    MessageEncodeAndStoreType(boolean oneMessageToManyIdsEncoding) {
        this.oneMessageToManyIdsEncoding = oneMessageToManyIdsEncoding;
    }

    /**
     * True if one message to many ids encoding is set
     * @return return oneMessageToManyIdsEncoding
     */
    public boolean useOneMessageToManyIdsEncoding() {
        return oneMessageToManyIdsEncoding;
    }
}


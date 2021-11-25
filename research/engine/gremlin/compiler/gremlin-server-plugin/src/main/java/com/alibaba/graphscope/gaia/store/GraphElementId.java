package com.alibaba.graphscope.gaia.store;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

public abstract class GraphElementId {
    private static final Logger logger = LoggerFactory.getLogger(GraphElementId.class);

    public static final int BYTE_SIZE = 16;

    public static byte[] toBytes(Object id) {
        logger.info("start to translate id");
        if (id instanceof Number) {
            return toBigEndian(new BigInteger(String.valueOf(id)));
        } else if (id instanceof String) {
            return toBigEndian(new BigInteger((String) id));
        } else if (id instanceof Element) {
            return GraphElementId.toBytes(String.valueOf(((Element) id).id()));
        } else {
            throw new UnsupportedOperationException("invalid id type " + id.getClass());
        }
    }

    // big-endian transformation
    private static byte[] toBigEndian(BigInteger value) {
        byte[] buffer = new byte[BYTE_SIZE];
        // initialize
        for (int i = 0; i < BYTE_SIZE; ++i) {
            buffer[i] = 0;
        }
        byte[] valueArray = value.toByteArray();
        // set big-endian
        int k = BYTE_SIZE - 1;
        for (int j = valueArray.length - 1; j >= 0; --j) {
            buffer[k] = valueArray[j];
            --k;
        }
        logger.info("input id buffer {}", buffer);
        return buffer;
    }

    public abstract Object fromBytes(byte[] edgeId);
}

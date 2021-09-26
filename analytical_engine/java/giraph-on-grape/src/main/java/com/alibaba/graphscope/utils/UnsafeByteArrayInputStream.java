package com.alibaba.graphscope.utils;

/**
 * UnsafeByteArrayInputStream
 *
 * This stream now extends com.esotericsoftware.kryo.io.Input so that kryo
 * serialization can directly read from this stream without using an
 * additional buffer, providing a faster serialization.

 * Users of this class has to explicitly close the stream to avoid style check
 * errors even though close is no-op when the underlying stream is not set.
 */
public class UnsafeByteArrayInputStream extends UnsafeArrayReads {

    /**
     * Constructor
     *
     * @param buf Buffer to read from
     */
    public UnsafeByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Constructor.
     *
     * @param buf Buffer to read from
     * @param offset Offsetin the buffer to start reading from
     * @param length Max length of the buffer to read
     */
    public UnsafeByteArrayInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }
}

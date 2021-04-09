package com.alibaba.maxgraph.v2.common.wal;

import java.io.IOException;

/**
 * A LogReader can read data of a queue from certain offset.
 */
public interface LogReader extends AutoCloseable {

    /**
     * Read the next LogEntry of the queue
     * @return
     */
    ReadLogEntry readNext();

    void close() throws IOException;
}

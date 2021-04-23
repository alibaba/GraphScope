package com.alibaba.maxgraph.v2.common.wal;

import java.io.IOException;

/**
 * A LogWriter can append data to a queue in the LogService.
 */
public interface LogWriter extends AutoCloseable {

    /**
     * Append {@link LogEntry} to the queue.
     * @param logEntry
     * @return
     */
    long append(LogEntry logEntry) throws IOException;

    void close() throws IOException;
}

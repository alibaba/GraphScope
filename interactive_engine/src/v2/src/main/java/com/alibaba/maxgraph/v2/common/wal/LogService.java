package com.alibaba.maxgraph.v2.common.wal;

import java.io.IOException;

/**
 * LogService provides persistent queues for realtime write.
 */
public interface LogService {

    void init();

    void destroy();

    boolean initialized();

    /**
     * Create a writer that can append data to a specific queue of LogService.
     * @param queueId
     * @return
     */
    LogWriter createWriter(int queueId);

    /**
     * Create a reader can read data of specific a queue from certain offset.
     * @param queueId
     * @param offset
     * @return
     */
    LogReader createReader(int queueId, long offset) throws IOException;

    /**
     * Delete all data before certain offset in the queue.
     *
     * @param queueId
     * @param offset
     */
    void deleteBeforeOffset(int queueId, long offset) throws IOException;
}

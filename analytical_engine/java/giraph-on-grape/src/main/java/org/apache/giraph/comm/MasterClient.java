package org.apache.giraph.comm;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;

public interface MasterClient {
    /**
     * Make sure that all the connections to workers have been established.
     */
    void openConnections();


    /**
     * Flush aggregated values cache.
     */
    void finishSendingValues() throws IOException;

    /**
     * Flush all outgoing messages.  This will synchronously ensure that all
     * messages have been send and delivered prior to returning.
     */
    void flush();

    /**
     * Send a request to a remote server (should be already connected)
     *
     * @param destTaskId Destination worker id
     * @param request Request to send
     */
    void sendWritableRequest(int destTaskId, FFIByteVector request);

    /**
     * Closes all connections.
     */
    void closeConnections();
}

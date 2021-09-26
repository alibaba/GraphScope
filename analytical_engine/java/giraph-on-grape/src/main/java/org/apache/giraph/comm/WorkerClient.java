package org.apache.giraph.comm;

import java.io.IOException;
import org.apache.giraph.comm.requests.WritableRequest;

public interface WorkerClient {
    /**
     * Make sure that all the connections to workers and master have been
     * established.
     */
    void openConnections();

    /**
     * Send a request to a remote server (should be already connected)
     *
     * @param destTaskId Destination worker id
     * @param request Request to send
     */
    void sendWritableRequest(int destTaskId, WritableRequest request);

    /**
     * Wait until all the outstanding requests are completed.
     */
    void waitAllRequests();

    /**
     * Closes all connections.
     *
     * @throws IOException
     */
    void closeConnections() throws IOException;

}

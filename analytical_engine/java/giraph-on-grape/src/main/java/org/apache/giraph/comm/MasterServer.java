package org.apache.giraph.comm;

import java.net.InetSocketAddress;

public interface MasterServer {
    /**
     * Get server address
     *
     * @return Address used by this server
     */
    InetSocketAddress getMyAddress();

    /**
     * Get server host name or IP
     * @return server host name or IP
     */
    String getLocalHostOrIp();

    /**
     * Prepare incoming messages for computation, and resolve mutation requests.
     */
    void prepareSuperstep();

    /**
     * Shuts down.
     */
    void close();
}

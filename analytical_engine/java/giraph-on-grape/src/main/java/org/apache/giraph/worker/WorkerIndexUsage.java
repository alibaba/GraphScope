package org.apache.giraph.worker;

/**
 * Interface providing utilities for using worker index.
 *
 * @param <OID_T> Vertex id type
 */
public interface WorkerIndexUsage<OID_T> {
    /**
     * Get number of workers
     *
     * @return Number of workers
     */
    int getWorkerCount();

    /**
     * Get index for this worker
     *
     * @return Index of this worker
     */
    int getMyWorkerIndex();

    /**
     * Get worker index which will contain vertex with given id,
     * if such vertex exists.
     *
     * @param vertexId vertex id
     * @return worker index
     */
    int getWorkerForVertex(OID_T vertexId);
}

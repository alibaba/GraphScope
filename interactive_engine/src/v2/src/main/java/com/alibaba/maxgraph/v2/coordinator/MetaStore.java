package com.alibaba.maxgraph.v2.coordinator;

import java.io.IOException;

/**
 * Distributed, reliable storage used for SnapshotManager to persist Snapshot related meta.
 * We can implement this interface with Zookeeper
 */
public interface MetaStore {

    boolean exists(String path);

    byte[] read(String path) throws IOException;

    void write(String path, byte[] content) throws IOException;

    void delete(String path) throws IOException;
}

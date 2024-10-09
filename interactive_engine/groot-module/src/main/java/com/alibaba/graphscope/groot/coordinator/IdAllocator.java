package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.common.exception.NotFoundException;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class IdAllocator {

    public static final String ID_ALLOCATE_INFO_PATH = "id_allocate_info";

    private final MetaStore metaStore;
    private final ObjectMapper objectMapper;
    private volatile long tailId;

    public IdAllocator(MetaStore metaStore) {
        this.metaStore = metaStore;
        this.objectMapper = new ObjectMapper();
    }

    public void start() {
        try {
            recover();
        } catch (IOException e) {
            throw new InternalException(e);
        }
    }

    public void stop() {
        // Do nothing
    }

    private void recover() throws IOException {
        if (!this.metaStore.exists(ID_ALLOCATE_INFO_PATH)) {
            throw new NotFoundException("File not found: " + ID_ALLOCATE_INFO_PATH);
        }
        byte[] b = this.metaStore.read(ID_ALLOCATE_INFO_PATH);
        this.tailId = this.objectMapper.readValue(b, Long.class);
    }

    public synchronized long allocate(int allocateSize) throws IOException {
        long newTailId = this.tailId + allocateSize;
        persistTailId(newTailId);
        this.tailId = newTailId;
        return newTailId;
    }

    private void persistTailId(long tailId) throws IOException {
        byte[] b = this.objectMapper.writeValueAsBytes(tailId);
        this.metaStore.write(ID_ALLOCATE_INFO_PATH, b);
    }
}

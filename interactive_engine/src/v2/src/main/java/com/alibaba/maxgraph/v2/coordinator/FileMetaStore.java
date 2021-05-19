package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.CoordinatorConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileMetaStore implements MetaStore {

    private String workingDir;

    public FileMetaStore(Configs configs) {
        this.workingDir = CoordinatorConfig.FILE_META_STORE_PATH.get(configs);
        new File(this.workingDir).mkdirs();
    }

    @Override
    public boolean exists(String path) {
        return Files.exists(new File(this.workingDir, path).toPath());
    }

    @Override
    public byte[] read(String path) throws IOException {
        return Files.readAllBytes(new File(this.workingDir, path).toPath());
    }

    @Override
    public void write(String path, byte[] content) throws IOException {
        Files.write(new File(this.workingDir, path).toPath(), content);
    }

    @Override
    public void delete(String path) throws IOException {
        Files.delete(new File(this.workingDir, path).toPath());
    }
}

/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.CoordinatorConfig;

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

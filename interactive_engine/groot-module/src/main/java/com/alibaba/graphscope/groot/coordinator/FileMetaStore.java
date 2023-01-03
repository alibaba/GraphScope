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

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.CoordinatorConfig;
import com.alibaba.graphscope.groot.meta.MetaStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class FileMetaStore implements MetaStore {
    private static final Logger logger = LoggerFactory.getLogger(FileMetaStore.class);

    private String workingDir;
    private Map<String, Integer> pathToSuffix;

    public FileMetaStore(Configs configs) {
        this.workingDir = CoordinatorConfig.FILE_META_STORE_PATH.get(configs);
        new File(this.workingDir).mkdirs();
        this.pathToSuffix = new ConcurrentHashMap<>();
    }

    private String pathWith(String path, int suffix) {
        return path + "." + suffix;
    }

    public static long getCRC32Checksum(byte[] bytes) {
        Checksum crc32 = new CRC32();
        crc32.update(bytes, 0, bytes.length);
        return crc32.getValue();
    }

    @Override
    public boolean exists(String path) {
        String realPath = pathWith(path, 0);
        return Files.exists(new File(this.workingDir, realPath).toPath());
    }

    @Override
    public byte[] read(String path) throws IOException {
        File file0 = new File(this.workingDir, pathWith(path, 0));
        File file1 = new File(this.workingDir, pathWith(path, 1));
        byte[] res;
        long timestamp0;
        try (InputStream is = new BufferedInputStream(new FileInputStream(file0))) {
            DataInputStream dataInputStream = new DataInputStream(is);
            long crc = dataInputStream.readLong();
            timestamp0 = dataInputStream.readLong();
            int length0 = dataInputStream.readInt();
            res = new byte[length0];
            is.read(res);
            long realCrc = getCRC32Checksum(res);
            if (realCrc != crc) {
                logger.error(
                        "checksum of file ["
                                + file0.getAbsolutePath()
                                + "] is ["
                                + realCrc
                                + "], expected ["
                                + crc
                                + "]");
                res = null;
            }
        }
        if (!file1.exists()) {
            if (res != null) {
                return res;
            } else {
                throw new IOException("file0 checksum failed, file1 not exists");
            }
        } else {
            try (InputStream is = new BufferedInputStream(new FileInputStream(file1))) {
                DataInputStream dataInputStream = new DataInputStream(is);
                long crc = dataInputStream.readLong();
                long timestamp1 = dataInputStream.readLong();
                if (timestamp1 <= timestamp0) {
                    if (res != null) {
                        return res;
                    } else {
                        throw new IOException("file0 checksum failed, file1 has old data");
                    }
                }
                int length1 = dataInputStream.readInt();
                res = new byte[length1];
                is.read(res);
                long realCrc = getCRC32Checksum(res);
                if (realCrc != crc) {
                    throw new IOException(
                            "checksum of file ["
                                    + file1.getAbsolutePath()
                                    + "] is ["
                                    + realCrc
                                    + "], expected ["
                                    + crc
                                    + "]");
                }
                return res;
            }
        }
    }

    @Override
    public void write(String path, byte[] content) throws IOException {
        Integer suffix = pathToSuffix.get(path);
        if (suffix == null) {
            suffix = 0;
        }
        pathToSuffix.put(path, 1 - suffix);
        File file = new File(this.workingDir, pathWith(path, suffix));
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            DataOutputStream dos = new DataOutputStream(os);
            dos.writeLong(getCRC32Checksum(content));
            dos.writeLong(System.currentTimeMillis());
            dos.writeInt(content.length);
            os.write(content);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        Files.deleteIfExists(new File(this.workingDir, pathWith(path, 0)).toPath());
        Files.deleteIfExists(new File(this.workingDir, pathWith(path, 1)).toPath());
    }
}

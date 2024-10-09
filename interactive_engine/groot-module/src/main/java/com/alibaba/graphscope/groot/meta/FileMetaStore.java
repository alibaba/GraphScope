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
package com.alibaba.graphscope.groot.meta;

import com.alibaba.graphscope.groot.common.exception.InvalidDataException;

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
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class FileMetaStore implements MetaStore {
    private static final Logger logger = LoggerFactory.getLogger(FileMetaStore.class);

    private final String workingDir;
    private final Map<String, Integer> pathToSuffix;

    public FileMetaStore(String metaPath) {
        this.workingDir = metaPath;
        boolean ret = new File(this.workingDir).mkdirs();
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
        File file0 = new File(workingDir, pathWith(path, 0));
        File file1 = new File(workingDir, pathWith(path, 1));
        return file0.exists() || file1.exists();
    }

    private SimpleImmutableEntry<Long, byte[]> readEntry(String path) throws IOException {
        File file = new File(this.workingDir, path);
        long timestamp = 0;
        byte[] res = null;
        if (file.exists()) {
            try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
                DataInputStream dataInputStream = new DataInputStream(is);
                long crc = dataInputStream.readLong();
                timestamp = dataInputStream.readLong();
                int length = dataInputStream.readInt();
                res = new byte[length];
                int ignored = is.read(res);
                long realCrc = getCRC32Checksum(res);
                if (realCrc != crc) {
                    throw new InvalidDataException(
                            "Checksum mismatch for " + file.getAbsolutePath());
                }
            }
        }
        return new SimpleImmutableEntry<>(timestamp, res);
    }

    @Override
    public byte[] read(String path) throws IOException {
        long timestamp0 = 0, timestamp1 = 0;
        byte[] res0 = null, res1 = null;
        String file0 = pathWith(path, 0), file1 = pathWith(path, 1);
        try {
            SimpleImmutableEntry<Long, byte[]> stat = readEntry(file0);
            timestamp0 = stat.getKey();
            res0 = stat.getValue();
        } catch (Exception e) {
            logger.error("Failed to read {}", file0, e);
        }
        try {
            SimpleImmutableEntry<Long, byte[]> stat = readEntry(file1);
            timestamp1 = stat.getKey();
            res1 = stat.getValue();
        } catch (Exception e) {
            logger.error("Failed to read {}", file1, e);
        }

        if (res0 != null && res1 != null) {
            return timestamp0 > timestamp1 ? res0 : res1;
        } else if (res0 != null) {
            return res0;
        } else if (res1 != null) {
            return res1;
        } else {
            throw new InvalidDataException("File maybe corrupted: " + path);
        }
    }

    @Override
    public void write(String path, byte[] content) throws IOException {
        Integer suffix = pathToSuffix.get(path);
        if (suffix == null) {
            suffix = 0;
        }
        pathToSuffix.put(path, 1 - suffix);
        String pathWithSuffix = pathWith(path, suffix);
        String tmpPath = pathWithSuffix + ".tmp";
        File file = new File(workingDir, pathWithSuffix);
        File tmpFile = new File(workingDir, tmpPath);
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(tmpFile))) {
            DataOutputStream dos = new DataOutputStream(os);
            dos.writeLong(getCRC32Checksum(content));
            dos.writeLong(System.currentTimeMillis());
            dos.writeInt(content.length);
            os.write(content);
        }
        Files.move(tmpFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void delete(String path) throws IOException {
        Files.deleteIfExists(new File(this.workingDir, pathWith(path, 0)).toPath());
        Files.deleteIfExists(new File(this.workingDir, pathWith(path, 1)).toPath());
    }
}

/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.dataload.databuild;

import org.rocksdb.*;

import java.io.IOException;
import java.util.*;

public class SstRecordWriter {
    private SstFileWriter sstFileWriter;
    private String charSet;
    private boolean isEmpty;

    public SstRecordWriter(String fileName, String charSet) throws IOException {
        this.isEmpty = true;
        this.charSet = charSet;
        Options options = new Options();
        options.setCreateIfMissing(true)
                .setWriteBufferSize(512 << 20)
                .setMaxWriteBufferNumber(8)
                .setTargetFileSizeBase(512 << 20);
        this.sstFileWriter = new SstFileWriter(new EnvOptions(), options);
        try {
            sstFileWriter.open(fileName);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public void write(String key, String value) throws IOException {
        this.isEmpty = false;
        try {
            sstFileWriter.put(key.getBytes(charSet), value.getBytes(charSet));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public boolean empty() {
        return isEmpty;
    }

    public void close() throws IOException {
        try {
            sstFileWriter.finish();
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }
}

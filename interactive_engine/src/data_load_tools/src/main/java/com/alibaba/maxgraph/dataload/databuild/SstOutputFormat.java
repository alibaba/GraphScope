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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SstOutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {

    private static final Logger logger = LoggerFactory.getLogger(SstOutputFormat.class);

    public static class SstRecordWriter extends RecordWriter<BytesWritable, BytesWritable> {

        private SstFileWriter sstFileWriter;
        private FileSystem fs;
        private String fileName;
        private Path path;

        public SstRecordWriter(FileSystem fs, Path path) throws RocksDBException {
            this.fs = fs;
            this.path = path;
            Options options = new Options();
            options.setCreateIfMissing(true)
                    .setWriteBufferSize(512<<20)
                    .setMaxWriteBufferNumber(8)
                    .setTargetFileSizeBase(512<<20);
            this.sstFileWriter = new SstFileWriter(new EnvOptions(), options);
            this.fileName = path.getName();
            sstFileWriter.open(fileName);
        }

        @Override
        public void write(BytesWritable key, BytesWritable value) throws IOException {
            try {
                sstFileWriter.put(key.copyBytes(), value.copyBytes());
            } catch (RocksDBException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            try {
                sstFileWriter.finish();
            } catch (RocksDBException e) {
                throw new IOException(e);
            }
            fs.copyFromLocalFile(true, new Path(fileName), path);
        }
    }

    @Override
    public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException {
        Configuration conf = job.getConfiguration();

        Path file = getDefaultWorkFile(job, ".sst");
        logger.info("output file [" + file.toString() + "]");
        FileSystem fs = file.getFileSystem(conf);
        try {
            return new SstRecordWriter(fs, file);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }
}

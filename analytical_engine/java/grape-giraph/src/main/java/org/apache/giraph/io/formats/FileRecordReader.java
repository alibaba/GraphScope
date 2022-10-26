/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.io.formats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Objects;

public class FileRecordReader extends RecordReader<LongWritable, Text> {

    private static Logger logger = LoggerFactory.getLogger(FileRecordReader.class);

    private BufferedReader reader;
    private String cachedLine;
    private Text reusableText;

    public void setReader(BufferedReader reader) {
        this.reader = reader;
        this.cachedLine = null;
        reusableText = new Text();
        logger.debug("set buffered reader: " + reader);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {}

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (Objects.isNull(cachedLine)) {
            cachedLine = reader.readLine();
        }
        //        logger.debug("check next line: {}", cachedLine);
        return (Objects.nonNull(cachedLine) && !cachedLine.isEmpty());
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(0);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        reusableText.set(cachedLine);
        //        logger.debug("Getting cur value: {}, text{}", cachedLine, reusableText);
        cachedLine = null;
        return reusableText;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

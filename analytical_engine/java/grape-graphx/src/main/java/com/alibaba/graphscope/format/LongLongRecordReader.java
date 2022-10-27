/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.format;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class LongLongRecordReader implements RecordReader<LongWritable, LongLong> {

    private Logger logger = LoggerFactory.getLogger(LongLongRecordReader.class.getName());
    private LineRecordReader lineRecordReader;
    private Text text = new Text();

    public LongLongRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter)
            throws IOException {
        lineRecordReader = new LineRecordReader(job, split, recordDelimiter);
    }

    @Override
    public boolean next(LongWritable longWritable, LongLong longLong) throws IOException {
        boolean res = lineRecordReader.next(longWritable, text);
        if (!res) {
            return false;
        }
        Iterator<String> iter =
                Splitter.on(CharMatcher.whitespace()).split(text.toString()).iterator();
        longLong.first = Long.parseLong(iter.next());
        longLong.second = Long.parseLong(iter.next());
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public LongLong createValue() {
        return new LongLong();
    }

    @Override
    public long getPos() throws IOException {
        return lineRecordReader.getPos();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }
}

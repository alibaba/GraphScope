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
package com.alibaba.graphscope.loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LoaderUtils {

    private static final int CODE_WIDTH = 4; // 4bits
    // Follows vineyard::type_to_int
    private static final int LONG_WRITABLE_CODE = 0x0004;
    private static final int INT_WRITABLE_CODE = 0x0002;
    private static final int DOUBLE_WRITABLE_CODE = 0x0007;
    private static final int FLOAT_WRITABLE_CODE = 0x0006;
    private static final int UDF_WRITABLE_CODE = 0x0009;
    private static final int NULL_WRITABLE_CODE = 0x0001;
    private static Logger logger = LoggerFactory.getLogger(LoaderUtils.class);

    public static boolean checkFileExist(String path) {
        File temp;
        temp = new File(path);
        return temp.exists();
    }
    public static long getNumLinesOfFile(String path) throws IOException {
        //if path start with hdfs://, we should use hadoop api to get the number of lines
        if (path.startsWith("hdfs://")) {
            return getNumLinesOfHdfsFile(path);
        }
        else {
            return getNumLinesOfLocalFile(path);
        }
    }

    public static long getNumLinesOfLocalFile(String path) {
        long count = 0;
        try {
            Path p = Paths.get(path);
            count = Files.lines(p).count();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public static String getEndpointFromPath(String path) {
        if (path.startsWith("hdfs://")) {
            int index = path.indexOf("/", 7);
            return path.substring(0, index);
        }
        return null;
    }

    public static BufferedReader createHdfsBufferedReader(String input) throws IOException {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(input);
        Configuration conf = new Configuration();
        String endpoint = getEndpointFromPath(input);
        logger.info("endpoint: " + endpoint);
        conf.set("fs.defaultFS", endpoint);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
        }
        catch (Exception e) {
            logger.error("Failed to get file system: " + input);
            e.printStackTrace();
            return null;
        }
        return new BufferedReader(new InputStreamReader(fileSystem.open(path)));
    }

    public static long getNumLinesOfHdfsFile(String input) throws IOException {
        BufferedReader reader = createHdfsBufferedReader(input);
        long count = 0;
        try {
            while (reader.readLine() != null) {
                count++;
            }
            reader.close();
        } catch (IOException e) {
            logger.error("Failed to read file: " + input);
            e.printStackTrace();
        }
        return count;
    }

    /**
     * Generate an int containing clz array info.
     *
     * @param clzs input classes.
     * @return generated value, the order of encoded bits value is the same as input order.
     */
    public static int generateTypeInt(Class<? extends Writable>... clzs) {
        if (clzs.length >= 32) {
            throw new IllegalStateException("expect less than 32 clzs");
        }
        int res = 0;
        for (Class<? extends Writable> clz : clzs) {
            res <<= CODE_WIDTH;
            res |= writable2Int(clz);
        }
        logger.info(Integer.toBinaryString(res));
        return res;
    }

    /**
     * return a 4 bit int containing type info.
     *
     * @param clz input clz
     * @return
     */
    public static int writable2Int(Class<? extends Writable> clz) {
        if (LongWritable.class.isAssignableFrom(clz)) {
            return LONG_WRITABLE_CODE;
        } else if (IntWritable.class.isAssignableFrom(clz)) {
            return INT_WRITABLE_CODE;
        } else if (DoubleWritable.class.isAssignableFrom(clz)) {
            return DOUBLE_WRITABLE_CODE;
        } else if (FloatWritable.class.isAssignableFrom(clz)) {
            return FLOAT_WRITABLE_CODE;
        } else if (NullWritable.class.isAssignableFrom(clz)) {
            return NULL_WRITABLE_CODE;
        }
        return UDF_WRITABLE_CODE;
    }
}

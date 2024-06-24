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

package com.alibaba.graphscope.utils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class.getName());

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

    public static long getNumLinesOfHdfsFile(String input) throws IOException {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(path.getFileSystem(new Configuration()).open(path)));
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
}

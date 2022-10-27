/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.io.formats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * The text output format used for Giraph text writing.
 */
public abstract class GiraphTextOutputFormat extends TextOutputFormat<Text, Text> {

    private static Logger logger = LoggerFactory.getLogger(GiraphTextOutputFormat.class);

    /**
     * This function returns a record writer according to provided configuration. Giraph write file
     * to hdfs.
     * <p>
     * In Grape-Giraph, we write to local file system.
     *
     * @param job shall be null.
     * @return created record writer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        String extension = "";
        //        String outputFileName = String
        //            .join("-", conf.getDefaultWorkerFile(), "frag",
        // String.valueOf(conf.getWorkerId()));

        String subdir = getSubdir();
        String outputFileFullPath;
        if (!subdir.isEmpty()) {
            if (checkDirExits(subdir)) {
                outputFileFullPath = String.join("/", subdir, getOutputFileName());
            } else {
                logger.error("Sub dir: " + subdir + " doesn't exists, or is not a directory");
                return null;
            }
        } else {
            outputFileFullPath =
                    String.join("/", System.getProperty("user.dir"), getOutputFileName());
        }

        String separator = "\t";

        logger.info("Create record writer destination: " + outputFileFullPath);

        DataOutputStream outputStream =
                new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(outputFileFullPath)));
        return new LineRecordWriter<Text, Text>(outputStream, separator);
    }

    /**
     * This function is used to provide an additional path level to keep different text outputs into
     * different directories.
     *
     * @return the subdirectory to be created under the output path
     */
    protected abstract String getSubdir();

    protected abstract String getOutputFileName();

    private boolean checkDirExits(String file) {
        File f = new File(file);
        if (f.exists() && f.isDirectory()) {
            return true;
        }
        return false;
    }
}

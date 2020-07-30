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
package com.alibaba.maxgraph.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @Author: peaker.lgf
 * @Email: peaker.lgf@alibaba-inc.com
 * @create: 2018-12-27 11:01
 **/
public class ZipUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ZipUtil.class);
    private static final int COMPRESS_LEVEL = 1;

    public ZipUtil() {}

    /**
     * Compress a directory to a outputStream of zip file, the zip file will contains all files in the directory, but not in a directory
     * @param directory directory, which must be a directory
     * @param outputStream
     * @throws IOException
     */
    public static void compress(String directory, OutputStream outputStream) throws IOException {
  //      Preconditions.checkArgument(directory.isDirectory(), directory.getName() + " is not a directory");
        LOG.info("create zip output stream...");
        System.out.println("compress:" + directory);
        ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(outputStream));
        zipOut.setLevel(COMPRESS_LEVEL);

        try {
            compressDirectory(directory, zipOut);
            zipOut.finish();
        } finally {
            zipOut.close();
        }
    }

    private static void compressDirectory(String path, final ZipOutputStream zipOutputStream) throws IOException {
        System.out.println(path);
        File file = new File(path);
        if(file.isFile()) {
            compressFile(path, zipOutputStream);
            return;
        } else if (file.isDirectory()){
            File[] array = file.listFiles();

            // process empty directory
            if(array.length < 1) {
                System.out.println("zip empty directory!!!");
                ZipEntry ze = new ZipEntry(path + File.separator);
                zipOutputStream.putNextEntry(ze);
                zipOutputStream.closeEntry();
            }
            for (int i = 0; i < array.length; i++) {
                if (array[i].isFile()) {
                    compressFile(path + File.separator + array[i].getName(), zipOutputStream);
                } else if (array[i].isDirectory()){
                    compressDirectory(path + File.separator + array[i].getName(), zipOutputStream);
                }
            }
        }
    }

    private static void compressFile(String path, final ZipOutputStream zipOutputStream) throws IOException {
        File file = new File(path);
        if(file.isFile()) {
            ZipEntry ze = new ZipEntry(path);
            zipOutputStream.putNextEntry(ze);
            Files.copy(file.toPath(), zipOutputStream);
            zipOutputStream.closeEntry();
        } else {
            LOG.warn("This is a directory: " + path);
        }
    }
}

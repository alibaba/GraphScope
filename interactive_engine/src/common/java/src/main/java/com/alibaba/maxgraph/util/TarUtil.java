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

import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarOutputStream;

import java.io.*;
import java.nio.file.Paths;

public class TarUtil {

    private static ThreadLocal<byte[]> buf = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[8<<10];
        }
    };

    public static void tar(String srcPath) throws IOException {
        tar(srcPath, srcPath + ".tar");
    }

    public static void tar(String srcPath, String dstPath) throws IOException {
        OutputStream outputStream = new FileOutputStream(dstPath);
        TarOutputStream out = new TarOutputStream(outputStream);
        tar(out, ".", new File(srcPath));
        out.close();
    }

    private static void tar(TarOutputStream out, String prefix, File file) throws IOException {
        if (file.isDirectory()) {
            String path = Paths.get(prefix, file.getName()).toString();
            for (File f : file.listFiles()) {
                tar(out, path, f);
            }
        } else {
            out.putNextEntry(new TarEntry(file, Paths.get(prefix, file.getName()).toString()));
            byte[] buf = TarUtil.buf.get();
            int count = 0;
            InputStream inputStream = new FileInputStream(file);
            while ((count = inputStream.read(buf)) != -1) {
                out.write(buf, 0, count);
            }
            inputStream.close();
            out.flush();
        }
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;

public class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class.getName());

    public static long getNumLinesOfFile(String path) {
        ProcessBuilder builder = new ProcessBuilder("wc", "-l", path);
        builder.inheritIO().redirectOutput(Redirect.PIPE);
        Process process = null;
        try {
            process = builder.start();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String res = reader.readLine().split("\\s+")[0];
                return Long.parseLong(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}

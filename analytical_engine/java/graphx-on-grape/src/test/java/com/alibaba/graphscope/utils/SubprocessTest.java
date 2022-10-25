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

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class SubprocessTest {

    @Test
    public void test2() throws IOException, InterruptedException {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.init();
        interpreter.runCommand("1");
        String res = interpreter.getResult();
        System.out.println(res);
        interpreter.runCommand("1 + 2");
        res = interpreter.getResult();
        System.out.println(res);
        interpreter.close();
        System.out.println("Finish test2");
    }

    @Test
    public void test1()
            throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        ProcessBuilder builder = new ProcessBuilder("/usr/bin/env", "python3", "-i");
        Process process = builder.start();

        new Thread(
                        () -> {
                            String line;
                            final BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(process.getInputStream()));
                            // Ignore line, or do something with it
                            while (true) {
                                try {
                                    if ((line = reader.readLine()) == null) {
                                        break;
                                    } else {
                                        System.out.println(line);
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        })
                .start();
        new Thread(
                        () -> {
                            final PrintWriter writer =
                                    new PrintWriter(
                                            new OutputStreamWriter(process.getOutputStream()));
                            writer.println("1");
                            writer.println("2 * 2");
                            writer.println("exit()");
                            writer.flush();
                        })
                .start();

        int res = process.waitFor();
        if (res != 0) {
            System.out.println("Terminate unexpectedly");
        }
    }
}

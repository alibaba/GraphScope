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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A python interpreter wrapped with java subprocess. We use this to executor graphscope python
 * code.
 */
public class PythonInterpreter {
    private final int MAX_TIME_WAIT_SECOND = 20;

    private Logger logger = LoggerFactory.getLogger(PythonInterpreter.class);
    private Process process;
    private BlockingQueue<String> outputQueue, inputQueue, errorQueue;
    private InterpreterOutputStream os;
    private InterpreterInputStream is;
    private InterpreterErrorStream errorStream;

    public PythonInterpreter() {}

    public void init() throws IOException {
        ProcessBuilder builder = new ProcessBuilder("/usr/bin/env", "python3", "-i");
        process = builder.start();
        outputQueue = new LinkedBlockingQueue<>();
        inputQueue = new LinkedBlockingQueue<>();
        errorQueue = new LinkedBlockingQueue<>();
        logger.info("Start process {}", process);
        os = new InterpreterOutputStream(process.getInputStream(), outputQueue);
        is = new InterpreterInputStream(process.getOutputStream(), inputQueue);
        errorStream = new InterpreterErrorStream(process.getErrorStream());
        os.start();
        is.start();
        errorStream.start();
    }

    public void runCommand(String str) {
        inputQueue.offer(str);
        logger.info("offering cmd str: {}", str);
    }

    public String getResult() throws InterruptedException {
        if (is.isAlive()) {
            logger.info("input stream thread alive, use take");
            return outputQueue.take();
        } else {
            logger.info("input stream thread dead, use poll");
            return outputQueue.poll();
        }
    }

    public String getMatched(String pattern) throws InterruptedException {
        String str;
        while (true) {
            str = outputQueue.take();
            if (str.contains(pattern)) {
                return str;
            } else {
                //                logger.info("got cmd output " + str + " but not matched");
                logger.info(str);
            }
        }
    }

    public void close() throws InterruptedException {
        is.end();
        logger.info("closing input stream thread");
        is.interrupt();
        os.join();
        errorStream.join();
        logger.info("");
    }

    public static class InterpreterInputStream extends Thread {

        private Logger logger = LoggerFactory.getLogger(InterpreterInputStream.class.getName());
        private PrintWriter writer;
        private BlockingQueue<String> queue;

        public InterpreterInputStream(OutputStream os, BlockingQueue<String> inputQueue) {
            writer = new PrintWriter(new OutputStreamWriter(os));
            queue = inputQueue;
        }

        @Override
        public void run() {
            String cmd;
            while (!interrupted()) {
                try {
                    cmd = queue.take();
                    writer.println(cmd);
                    logger.info("Submitting command: {}", cmd);
                    writer.flush();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void end() {
            boolean res = queue.offer("exit()");
            if (!res) {
                throw new IllegalStateException("exit from subprocess failed");
            }
        }
    }

    public static class InterpreterOutputStream extends Thread {

        private Logger logger = LoggerFactory.getLogger(InterpreterOutputStream.class.getName());
        private BufferedReader reader;
        private BlockingQueue<String> queue;

        public InterpreterOutputStream(InputStream is, BlockingQueue<String> queue) {
            reader = new BufferedReader(new InputStreamReader(is));
            this.queue = queue;
        }

        @Override
        public void run() {
            String line;
            int cnt = 0;
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) {
                        break;
                    } else {
                        queue.add(line);
                        cnt += 1;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            logger.info("totally {} lines were read from interpreter", cnt);
        }
    }

    public static class InterpreterErrorStream extends Thread {

        private Logger logger = LoggerFactory.getLogger(InterpreterErrorStream.class.getName());
        private BufferedReader reader;

        public InterpreterErrorStream(InputStream is) {
            reader = new BufferedReader(new InputStreamReader(is));
        }

        @Override
        public void run() {
            String line;
            int cnt = 0;
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) {
                        break;
                    } else {
                        logger.info("Error Stream: {}", line);
                        cnt += 1;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            logger.info("totally {} lines were read from error stream", cnt);
        }
    }
}

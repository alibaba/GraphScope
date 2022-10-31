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

import com.alibaba.graphscope.graphx.utils.GrapeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MPIUtils {

    private static Logger logger = LoggerFactory.getLogger(MPIUtils.class.getName());
    private static final String GRAPHSCOPE_HOME;
    private static final String LAUNCH_GRAPHX_SHELL_SCRIPT;
    private static final String Load_FRAGMENT_PATTERN = "ArrowProjectedFragmentID:";
    private static final String FINALIZE_PATTERN = "Workers finalized.";
    private static final String LOAD_FRAGMENT_TASK = "load_fragment";
    private static final String GRAPHX_PREGEL_TASK = "run_pregel";

    public static class CommendBuilder {
        private List<String> commands;

        public CommendBuilder() {
            commands = new ArrayList<>();
            commands.add("/bin/bash");
            commands.add(LAUNCH_GRAPHX_SHELL_SCRIPT);
        }

        public String[] build() {
            String[] res = new String[commands.size()];
            commands.toArray(res);
            return res;
        }

        public CommendBuilder numWorkers(int numWorker) {
            commands.add("--num-workers");
            commands.add(String.valueOf(numWorker));
            return this;
        }

        public CommendBuilder task(String task) {
            commands.add("--task");
            commands.add(task);
            return this;
        }

        public CommendBuilder hostSlot(String hostSlot) {
            commands.add("--host-slot");
            commands.add(hostSlot);
            return this;
        }

        public CommendBuilder vdClass(String vdClass) {
            commands.add("--vd-class");
            commands.add(vdClass);
            return this;
        }

        public CommendBuilder edClass(String edClass) {
            commands.add("--ed-class");
            commands.add(edClass);
            return this;
        }

        public CommendBuilder msgClass(String msgClass) {
            commands.add("--msg-class");
            commands.add(msgClass);
            return this;
        }

        public CommendBuilder serialPath(String serialPath) {
            commands.add("--serial-path");
            commands.add(serialPath);
            return this;
        }

        public CommendBuilder fragIds(String[] fragIds) {
            commands.add("--frag-ids");
            commands.add(String.join(",", fragIds));
            return this;
        }

        public CommendBuilder rawDataIds(String[] rawDataIds) {
            commands.add("--raw-data-ids");
            commands.add(String.join(",", rawDataIds));
            return this;
        }

        public CommendBuilder maxIteration(int maxIteration) {
            commands.add("--max-iteration");
            commands.add(String.valueOf(maxIteration));
            return this;
        }

        public CommendBuilder numPart(int numPart) {
            commands.add("--num-part");
            commands.add(String.valueOf(numPart));
            return this;
        }

        public CommendBuilder ipcSocket(String ipcSocket) {
            commands.add("--ipc-socket");
            commands.add(ipcSocket);
            return this;
        }

        public CommendBuilder userJarPath(String userJarPath) {
            commands.add("--user-jar-path");
            commands.add(userJarPath);
            return this;
        }
    }

    static {
        GRAPHSCOPE_HOME = System.getenv("GRAPHSCOPE_HOME");
        if (GRAPHSCOPE_HOME == null || GRAPHSCOPE_HOME.isEmpty()) {
            throw new IllegalStateException("GRAPHSCOPE_HOME empty");
        }

        LAUNCH_GRAPHX_SHELL_SCRIPT = GRAPHSCOPE_HOME + "/bin/run_graphx.sh";
        if (!fileExists(LAUNCH_GRAPHX_SHELL_SCRIPT)) {
            throw new IllegalStateException(
                    "script " + LAUNCH_GRAPHX_SHELL_SCRIPT + "doesn't exist");
        }
    }

    public static <MSG, VD, ED> List<String> launchGraphX(
            String[] fragIds,
            Class<? extends VD> vdClass,
            Class<? extends ED> edClass,
            Class<? extends MSG> msgClass,
            String serialPath,
            int maxIteration,
            int numPart,
            String ipcSocket,
            String userJarPath) {
        int numWorkers = fragIds.length;
        String hostNameSlots = generateHostNameAndSlotsFromIDs(fragIds);
        logger.info("running mpi with {} workers", numWorkers);
        List<String> vertexDataIds = new ArrayList<>(numWorkers);
        String[] commands =
                new CommendBuilder()
                        .task(GRAPHX_PREGEL_TASK)
                        .numWorkers(numWorkers)
                        .hostSlot(hostNameSlots)
                        .vdClass(GrapeUtils.classToStr(vdClass, true))
                        .edClass(GrapeUtils.classToStr(edClass, true))
                        .msgClass(GrapeUtils.classToStr(msgClass, true))
                        .serialPath(serialPath)
                        .fragIds(fragIds)
                        .maxIteration(maxIteration)
                        .numPart(numPart)
                        .ipcSocket(ipcSocket)
                        .userJarPath(userJarPath)
                        .build();

        logger.info("Running with commands: " + String.join(" ", commands));
        long startTime = System.nanoTime();
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        //        processBuilder.inheritIO();
        Process process = null;
        try {
            process = processBuilder.start();
            BufferedReader stdInput =
                    new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String str;
            int cnt = 0;
            while ((str = stdInput.readLine()) != null) {
                System.out.println(str);
                if (str.contains(FINALIZE_PATTERN)) {
                    cnt += 1;
                }
                if (cnt >= numWorkers) {
                    logger.info("Got all workers in finalized, eager finish with kill");
                    break;
                }
            }
            if (cnt >= numWorkers) {
                process.destroyForcibly();
                logger.info("kill mpi processes forcibly");
            } else {
                int exitCode = process.waitFor();
                logger.info("Mpi process exit code {}", exitCode);
                if (exitCode != 0) {
                    throw new IllegalStateException("Error in mpi process" + exitCode);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        logger.info(
                "Total time spend on running mpi processes : {}ms",
                (endTime - startTime) / 1000000);
        return vertexDataIds;
    }

    private static void check(String oidType, String vidType) {
        if (oidType != "int64_t" || vidType != "uint64_t") {
            throw new IllegalStateException("Not supported: " + oidType + " " + vidType);
        }
    }

    /**
     * Input : d50:0:123457,d51:1:1232431 Output d50:1,d51:1
     *
     * @param ids
     * @return
     */
    private static String generateHostNameAndSlotsFromIDs(String[] ids) {
        HashMap<String, Integer> map = new HashMap<>();
        for (String str : ids) {
            String[] splited = str.split(":");
            if (splited.length != 3) {
                throw new IllegalStateException("Unexpected input " + Arrays.toString(ids));
            }
            if (map.containsKey(splited[0])) {
                map.put(splited[0], map.get(splited[0]) + 1);
            } else {
                map.put(splited[0], 1);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String key : map.keySet()) {
            Integer value = map.get(key);
            sb.append(key);
            sb.append(":");
            sb.append(value);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static <VD, ED> String[] loadFragment(
            String[] rawDataIds,
            String ipcSocket,
            Class<? extends VD> vdClass,
            Class<? extends ED> edClass) {
        logger.info("Try to load fragment from raw datas: {}", Arrays.toString(rawDataIds));
        int numWorkers = rawDataIds.length;
        logger.info("running mpi with {} workers", numWorkers);
        String hostNameAndSlots = generateHostNameAndSlotsFromIDs(rawDataIds);
        String[] commands =
                new CommendBuilder()
                        .task(LOAD_FRAGMENT_TASK)
                        .numWorkers(numWorkers)
                        .hostSlot(hostNameAndSlots)
                        .rawDataIds(rawDataIds)
                        .ipcSocket(ipcSocket)
                        .vdClass(GrapeUtils.classToStr(vdClass, true))
                        .edClass(GrapeUtils.classToStr(edClass, true))
                        .build();
        logger.info("Running with commands: " + String.join(" ", commands));
        String[] fragIds = new String[numWorkers];
        long startTime = System.nanoTime();
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        Process process = null;
        int ind = 0;
        try {
            process = processBuilder.start();
            BufferedReader stdInput =
                    new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String str;
            while ((str = stdInput.readLine()) != null) {
                System.out.println(str);
                if (str.contains(Load_FRAGMENT_PATTERN)) {
                    if (ind >= numWorkers) {
                        throw new IllegalStateException(
                                "Matched "
                                        + ind
                                        + " frag, but we only need "
                                        + numWorkers
                                        + " frags");
                    }
                    fragIds[ind] =
                            (str.substring(
                                            str.indexOf(Load_FRAGMENT_PATTERN)
                                                    + Load_FRAGMENT_PATTERN.length())
                                    .trim());
                    ind += 1;
                }
            }
            int exitCode = process.waitFor();
            logger.info("Mpi process exit code {}", exitCode);
            if (exitCode != 0) {
                throw new IllegalStateException("Error in mpi process" + exitCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        logger.info(
                "Total time spend on Loading global vertex Map : {}ms",
                (endTime - startTime) / 1000000);
        return fragIds;
    }

    private static boolean fileExists(String p) {
        return Files.exists(Paths.get(p));
    }
}

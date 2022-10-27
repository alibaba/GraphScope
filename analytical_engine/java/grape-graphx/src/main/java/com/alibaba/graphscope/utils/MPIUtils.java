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
    private static final String GRAPHSCOPE_HOME, SPARK_HOME;
    private static final String CONSTRUCT_GRAPHX_VERTEX_MAP_SHELL_SCRIPT,
            LAUNCH_GRAPHX_SHELL_SCRIPT;
    private static final String VM_PATTERN = "GlobalVertexMapID:";
    private static final String FINALIZE_PATTERN = "Workers finalized.";
    private static final String CONSTRUCT_GLOBAL_VM_TASK = "construct_vertex_map";
    private static final String GRAPHX_PREGEL_TASK = "graphx_pregel";

    static {
        SPARK_HOME = System.getenv("SPARK_HOME");
        if (SPARK_HOME == null || SPARK_HOME.isEmpty()) {
            throw new IllegalStateException("SPARK_HOME empty");
        }

        GRAPHSCOPE_HOME = System.getenv("GRAPHSCOPE_HOME");
        if (GRAPHSCOPE_HOME == null || GRAPHSCOPE_HOME.isEmpty()) {
            throw new IllegalStateException("GRAPHSCOPE_HOME empty");
        }

        LAUNCH_GRAPHX_SHELL_SCRIPT = GRAPHSCOPE_HOME + "/bin/run_graphx.sh";
        if (!fileExists(LAUNCH_GRAPHX_SHELL_SCRIPT)) {
            throw new IllegalStateException(
                    "script " + LAUNCH_GRAPHX_SHELL_SCRIPT + "doesn't exist");
        }
        CONSTRUCT_GRAPHX_VERTEX_MAP_SHELL_SCRIPT =
                GRAPHSCOPE_HOME + "/bin/construct_graphx_vertex_map.sh";
        if (!fileExists(CONSTRUCT_GRAPHX_VERTEX_MAP_SHELL_SCRIPT)) {
            throw new IllegalStateException(
                    "script " + CONSTRUCT_GRAPHX_VERTEX_MAP_SHELL_SCRIPT + "doesn't exist");
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
        String[] commands = {
            "/bin/bash",
            LAUNCH_GRAPHX_SHELL_SCRIPT,
            GRAPHX_PREGEL_TASK,
            String.valueOf(numWorkers),
            hostNameSlots,
            GrapeUtils.classToStr(vdClass, true),
            GrapeUtils.classToStr(edClass, true),
            GrapeUtils.classToStr(msgClass, true),
            serialPath,
            String.join(",", fragIds),
            String.valueOf(maxIteration),
            String.valueOf(numPart),
            ipcSocket,
            userJarPath
        };

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

    public static <MSG, VD, ED> List<String> constructGlobalVM(
            String[] localVMIDs, String ipcSocket, String oidType, String vidType) {
        check(oidType, vidType);
        logger.info("Try to construct global vm from: {}", Arrays.toString(localVMIDs));
        int numWorkers = localVMIDs.length;
        logger.info("running mpi with {} workers", numWorkers);
        String hostNameAndSlots = generateHostNameAndSlotsFromIDs(localVMIDs);
        String[] commands = {
            "/bin/bash",
            CONSTRUCT_GRAPHX_VERTEX_MAP_SHELL_SCRIPT,
            CONSTRUCT_GLOBAL_VM_TASK,
            String.valueOf(numWorkers),
            hostNameAndSlots,
            String.join(",", localVMIDs),
            ipcSocket
        };
        logger.info("Running with commands: " + String.join(" ", commands));
        List<String> globalVMIDs = new ArrayList<>(numWorkers);
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
            while ((str = stdInput.readLine()) != null) {
                System.out.println(str);
                if (str.contains(VM_PATTERN)) {
                    globalVMIDs.add(
                            (str.substring(str.indexOf(VM_PATTERN) + VM_PATTERN.length()).trim()));
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
        return globalVMIDs;
    }

    private static boolean fileExists(String p) {
        return Files.exists(Paths.get(p));
    }
}

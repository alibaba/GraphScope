/**
 * Copyright 2024 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.gaia.benchmark;

import com.alibaba.graphscope.gaia.common.Configuration;
import com.alibaba.graphscope.gaia.utils.PropertyUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CollectResult {

    static class QueryResult {
        String queryName;
        List<Long> executionTimes;

        QueryResult(String queryName) {
            this.queryName = queryName;
            this.executionTimes = new ArrayList<>();
        }

        void addExecutionTime(long time) {
            executionTimes.add(time);
        }

        int count() {
            return executionTimes.size();
        }

        double average() {
            return executionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }

        long percentile(int percent) {
            Collections.sort(executionTimes);
            int index = (int) Math.ceil(percent / 100.0 * executionTimes.size()) - 1;
            return executionTimes.get(Math.min(index, executionTimes.size() - 1));
        }
    }

    public static void analyzeLog(String logFilePath, String reportFilePath) {
        Map<String, Map<String, QueryResult>> systemQueryResults = new LinkedHashMap<>();
        String currentSystem = "";

        Pattern queryNamePattern = Pattern.compile("QueryName\\[(.*?)\\]");

        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("Start to benchmark system: ")) {
                    currentSystem = line.split(": ")[1].trim();
                } else {
                    Matcher matcher = queryNamePattern.matcher(line);
                    if (matcher.find()) {
                        String queryName = matcher.group(1).trim();
                        if (line.contains("ExecuteTimeMS")) {
                            long execTime =
                                    Long.parseLong(
                                            line.split("ExecuteTimeMS\\[")[1].split("\\]")[0]);
                            systemQueryResults.putIfAbsent(currentSystem, new HashMap<>());
                            systemQueryResults
                                    .get(currentSystem)
                                    .putIfAbsent(queryName, new QueryResult(queryName));
                            systemQueryResults
                                    .get(currentSystem)
                                    .get(queryName)
                                    .addExecutionTime(execTime);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        printResults(systemQueryResults, reportFilePath);
    }

    private static void printResults(
            Map<String, Map<String, QueryResult>> systemQueryResults, String reportFilePath) {
        StringBuilder sb = new StringBuilder();

        // Header
        sb.append("| QueryName | ")
                .append(
                        systemQueryResults.keySet().stream()
                                .flatMap(
                                        system ->
                                                Arrays.stream(
                                                        new String[] {
                                                            system + " Avg", system + " P50",
                                                            system + " P90", system + " P95",
                                                            system + " P99", system + " Count"
                                                        }))
                                .collect(Collectors.joining(" | ")))
                .append(" |\n");

        // Divider
        sb.append("| --------- | ")
                .append(
                        systemQueryResults.keySet().stream()
                                        .flatMap(
                                                system ->
                                                        Arrays.stream(
                                                                new String[] {
                                                                    "---------", "---------",
                                                                    "---------", "---------",
                                                                    "---------", "---------"
                                                                }))
                                        .collect(Collectors.joining(" | "))
                                + " |\n");

        // Sorted Query Names
        List<String> sortedQueryNames =
                systemQueryResults.values().stream()
                        .flatMap(map -> map.keySet().stream())
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList());

        for (String queryName : sortedQueryNames) {
            sb.append("| ").append(queryName).append(" |");
            for (String system : systemQueryResults.keySet()) {
                QueryResult result = systemQueryResults.get(system).get(queryName);
                if (result != null) {
                    sb.append(String.format(" %-2.2f |", result.average()));
                    sb.append(String.format(" %-2d |", result.percentile(50)));
                    sb.append(String.format(" %-2d |", result.percentile(90)));
                    sb.append(String.format(" %-2d |", result.percentile(95)));
                    sb.append(String.format(" %-2d |", result.percentile(99)));
                    sb.append(String.format(" %-2d |", result.count()));
                } else {
                    sb.append(" N/A | N/A | N/A | N/A | N/A | N/A |");
                }
            }
            sb.append("\n");
        }

        if (reportFilePath.isEmpty()) {
            System.out.println(sb.toString());
        } else {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportFilePath))) {
                writer.write(sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error, Usage: <interactive-benchmark.properties>");
            return;
        }
        Properties properties = PropertyUtil.getProperties(args[0], false);
        Configuration configuration = new Configuration(properties);
        String logPath = configuration.getString(Configuration.BENCH_RESULT_LOG_PATH, "");
        String reportPath = configuration.getString(Configuration.BENCH_RESULT_REPORT_PATH, "");
        analyzeLog(logPath, reportPath);
    }
}

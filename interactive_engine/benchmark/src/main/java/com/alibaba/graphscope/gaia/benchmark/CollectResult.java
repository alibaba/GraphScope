package com.alibaba.graphscope.gaia.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
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

    public static void analyzeLog(String logFilePath) {
        Map<String, Map<String, QueryResult>> systemQueryResults = new LinkedHashMap<>();
        String currentSystem = "";

        Pattern queryNamePattern = Pattern.compile("QueryName\\[(.*?)\\]");

        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Start to benchmark system: ")) {
                    currentSystem = line.split(": ")[1].trim();
                } else {
                    Matcher matcher = queryNamePattern.matcher(line);
                    if (matcher.find()) {
                        String queryName = matcher.group(1).trim(); // 获取匹配到的 queryName
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
        printResults(systemQueryResults);
    }

    private static void printResults(Map<String, Map<String, QueryResult>> systemQueryResults) {
        System.out.println(
                "| QueryName | "
                        + systemQueryResults.keySet().stream()
                                .flatMap(
                                        system ->
                                                Arrays.stream(
                                                        new String[] {
                                                            system + " Avg",
                                                            system + " P50",
                                                            system + " P90",
                                                            system + " P95",
                                                            system + " P99",
                                                            system + " Count"
                                                        }))
                                .collect(Collectors.joining(" | "))
                        + " |");

        System.out.println(
                "|-----------| "
                        + systemQueryResults.keySet().stream()
                                .map(
                                        system ->
                                                "|----------|----------|----------|----------|----------|")
                                .collect(Collectors.joining(" "))
                        + " |");

        List<String> sortedQueryNames =
                systemQueryResults.values().stream()
                        .flatMap(map -> map.keySet().stream())
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList());

        for (String queryName : sortedQueryNames) {
            System.out.printf("| %-2s |", queryName);
            for (String system : systemQueryResults.keySet()) {
                QueryResult result = systemQueryResults.get(system).get(queryName);
                if (result != null) {
                    System.out.printf(" %-2.2f |", result.average());
                    System.out.printf(" %-2d |", result.percentile(50));
                    System.out.printf(" %-2d |", result.percentile(90));
                    System.out.printf(" %-2d |", result.percentile(95));
                    System.out.printf(" %-2d |", result.percentile(99));
                    System.out.printf(" %-2d |", result.count());
                } else {
                    System.out.printf(" %-2s |", "N/A");
                    System.out.printf(" %-2s |", "N/A");
                    System.out.printf(" %-2s |", "N/A");
                    System.out.printf(" %-2s |", "N/A");
                    System.out.printf(" %-2s |", "N/A");
                    System.out.printf(" %-2s |", "N/A");
                }
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        analyzeLog(
                "./log/benchmark.log");
    }
}

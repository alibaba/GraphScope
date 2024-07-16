package com.alibaba.graphscope.gaia.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class BenchmarkResultComparator {
    private Map<String, String> expectedResults;

    public BenchmarkResultComparator(String expectedResultsPath) {
        if (expectedResultsPath != null) {
            try (Reader reader = Files.newBufferedReader(Paths.get(expectedResultsPath)); ) {
                Gson gson = new Gson();
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                this.expectedResults = gson.fromJson(reader, type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load expected results file.", e);
            }
        }
    }

    public BenchmarkResultComparator(Map<String, String> expectedResults) {
        this.expectedResults = expectedResults;
    }

    public void compareResults(String queryName, String actualResult) {
        String expectedResult = expectedResults.getOrDefault(queryName, "").trim();
        if (!expectedResult.isEmpty()) {
            if (normalizeString(expectedResult).equals(normalizeString(actualResult))) {
                System.out.println(queryName + ": Query result matches the expected result.");
            } else {
                System.err.println(
                        queryName + ": Query result does not match the expected result.");
                System.err.println("Expected: " + expectedResult);
                System.err.println("Actual  : " + actualResult);
            }
        } else {
            System.err.println(queryName + ": No expected result found for comparison.");
        }
    }

    public boolean isEmpty() {
        return expectedResults == null || expectedResults.isEmpty();
    }

    private String normalizeString(String str) {
        str = str.replace("\\\"", "\"");
        str = str.replace("'", "\"");
        str = str.replace("None", "Null");
        str = str.replaceAll("\\s+", " ");
        return str.trim();
    }
}

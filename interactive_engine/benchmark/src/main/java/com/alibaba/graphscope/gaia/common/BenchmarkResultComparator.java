package com.alibaba.graphscope.gaia.common;

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

    public void compareResults(String queryName, String actualResult) {
        String expectedValue = expectedResults.getOrDefault(queryName, "").trim();
        if (actualResult.trim().equals(expectedValue)) {
            System.out.println(queryName + " - Result matches expected output.");
        } else {
            System.out.println(queryName + " - Result does not match expected output.");
            System.out.println("Actual result: " + actualResult);
            System.out.println("Expected result: " + expectedValue);
        }
    }

    public boolean isEmpty() {
        return expectedResults == null || expectedResults.isEmpty();
    }
}

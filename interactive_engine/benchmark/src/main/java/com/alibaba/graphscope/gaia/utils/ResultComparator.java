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
package com.alibaba.graphscope.gaia.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class ResultComparator {
    private Map<String, String> expectedResults;
    private static Logger logger = LoggerFactory.getLogger(ResultComparator.class);

    public ResultComparator(String expectedResultsPath) {
        if (expectedResultsPath != null && !expectedResultsPath.isEmpty()) {
            try (Reader reader = Files.newBufferedReader(Paths.get(expectedResultsPath)); ) {
                Gson gson = new Gson();
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                this.expectedResults = gson.fromJson(reader, type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load expected results file.", e);
            }
        }
    }

    public ResultComparator(Map<String, String> expectedResults) {
        this.expectedResults = expectedResults;
    }

    public void compareResults(String queryName, String actualResult) {
        String expectedResult = expectedResults.getOrDefault(queryName, "").trim();
        if (!expectedResult.isEmpty()) {
            if (normalizeString(expectedResult).equals(normalizeString(actualResult))) {
                logger.info(queryName + ": Query result matches the expected result.");
            } else {
                logger.error(queryName + ": Query result does not match the expected result.");
                logger.error("Expected: " + expectedResult);
                logger.error("Actual  : " + actualResult);
            }
        } else {
            logger.error(queryName + ": No expected result found for comparison.");
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

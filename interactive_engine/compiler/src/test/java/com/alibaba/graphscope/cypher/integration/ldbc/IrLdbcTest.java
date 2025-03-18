/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.cypher.integration.ldbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IrLdbcTest {
    private static final Logger logger = LoggerFactory.getLogger(IrLdbcTest.class);
    private static Session session;

    @BeforeClass
    public static void beforeClass() {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_ldbc_tests() throws Exception {
        String queryDir =
                System.getProperty("query.dir", "../../flex/resources/queries/ic/runtime");
        File dir = new File(queryDir);
        int totalTests = 0;
        int passedTests = 0;
        for (File file : dir.listFiles()) {
            String queryName;
            try {
                if (file.getName().endsWith(".cypher")) {
                    ++totalTests;
                    queryName = file.getName().substring(0, file.getName().length() - 7);
                    String query = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
                    String results =
                            FileUtils.readFileToString(
                                    new File(Path.of(queryDir, queryName + ".json").toString()),
                                    StandardCharsets.UTF_8);
                    Map<String, Object> resultMap =
                            new ObjectMapper().readValue(results, Map.class);
                    query = renderQuery(query, (Map<String, Object>) resultMap.get("operation"));
                    List<String> errors = Lists.newArrayList();
                    Object result = resultMap.get("result");
                    if (!(result instanceof List)) {
                        result = Lists.newArrayList(result);
                    }
                    boolean equals =
                            checkResults(
                                    queryName, query, session.run(query), (List) result, errors);
                    if (equals) {
                        ++passedTests;
                        logger.warn("Test {} passed", queryName);
                    } else {
                        logger.error("Test {} failed. Errors: {}", queryName, errors);
                    }
                }
            } catch (Exception e) {
                logger.error("Test {} failed due to unexpected exception: ", file.getName(), e);
            }
        }
        if (passedTests != totalTests) {
            throw new IllegalArgumentException(
                    "Total Tests " + totalTests + ", Passed Tests " + passedTests);
        }
        logger.warn("All Tests Passed");
    }

    private String renderQuery(String query, Map<String, Object> params) throws Exception {
        for (Map.Entry<String, Object> param : params.entrySet()) {
            query = query.replace("$" + param.getKey(), param.getValue().toString());
        }
        return query;
    }

    private boolean checkResults(
            String queryName, String query, Result result, List expected, List<String> errors) {
        Iterator expectedIt = expected.iterator();
        while (result.hasNext() && expectedIt.hasNext()) {
            if (!checkRecord(
                    queryName, result.next(), (Map<String, Object>) expectedIt.next(), errors)) {
                return false;
            }
        }
        if (result.hasNext()) {
            errors.add(
                    String.format(
                            "QueryName: %s, Redundant record. No matching expected result was found"
                                    + " for record %s",
                            queryName, result.next()));
            return false;
        }
        if (expectedIt.hasNext()) {
            errors.add(
                    String.format(
                            "QueryName: %s, Missing record. No record was found to match the"
                                    + " expected result %s",
                            queryName, expectedIt.next()));
            return false;
        }
        return true;
    }

    private boolean checkRecord(
            String queryName, Record record, Map<String, Object> expected, List<String> errors) {
        Map<String, Object> recordMap = record.asMap();
        if (!recordMap.toString().equals(expected.toString())) {
            errors.add(
                    String.format(
                            "QueryName: %s, Record not equal. Record: %s vs Expected: %s",
                            queryName, recordMap, expected));
            return false;
        }
        return true;
    }
}

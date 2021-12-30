/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
package com.alibaba.maxgraph.function.test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    /**
     * @param endpoint manager server url
     * @param mode     create/delete
     * @param paras
     * @return json formatted
     * @throws Exception
     */
    public static String curlHttp(String endpoint, String mode, Map<String, String> paras)
            throws GraphTestException {
        try {
            List<String> valuePairs =
                    paras.entrySet().stream()
                            .map(k -> new String(k.getKey() + "=" + k.getValue()))
                            .collect(Collectors.toList());
            String paraStr = String.join("&", valuePairs);
            System.out.println(String.format("http://%s/instance/%s?%s", endpoint, mode, paraStr));
            URL url = new URL(String.format("http://%s/instance/%s?%s", endpoint, mode, paraStr));
            return IOUtils.toString(url.openStream(), "utf-8");
        } catch (Exception e) {
            throw new GraphTestException(e);
        }
    }

    /**
     * @param commands shell and args
     * @return error code and message
     */
    public static Pair<Integer, String> runShellCmd(String workDir, String... commands) {
        try {
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.directory(new File(workDir));
            Process process = pb.start();

            List<String> infoValueList = IOUtils.readLines(process.getInputStream(), "UTF-8");
            int errorCode = process.waitFor();
            if (errorCode == 0 && infoValueList != null && infoValueList.size() == 1) {
                return Pair.of(errorCode, infoValueList.get(0));
            }
        } catch (Exception e) {
            logger.error("exception is " + e);
        }
        return Pair.of(-1, null);
    }

    public static Map<String, Object> getValuePairs(String jsonStr, String... keys) {
        JSONObject object = new JSONObject(jsonStr);
        Map<String, Object> res = new HashMap<>();
        for (String key : keys) {
            res.put(key, object.get(key));
        }
        return res;
    }

    public static InputStream getLoadlResource(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    }

    public static String resourceAbsPath(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResource(fileName).getPath();
    }
}

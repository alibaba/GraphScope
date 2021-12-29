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
package com.alibaba.maxgraph.function.test.config;

import com.alibaba.maxgraph.function.test.GraphTestException;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * accept a config file and render from system properties, replace all '${}' with system variable value
 */
public class ConfigTemplateRender {
    private InputStream inputStream;

    public ConfigTemplateRender(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public Properties renderFromSysEnv() throws GraphTestException {
        try {
            Properties output = new Properties();
            Properties input = new Properties();
            input.load(inputStream);
            for (Map.Entry<Object, Object> entry : input.entrySet()) {
                output.setProperty(
                        entry.getKey().toString(), parseFromSysEnv(entry.getValue().toString()));
            }
            return output;
        } catch (Exception e) {
            throw new GraphTestException(e);
        }
    }

    private String parseFromSysEnv(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); ) {
            char ch = value.charAt(i);
            if (ch == '$' && ++i < value.length() && value.charAt(i) == '{') {
                int j = i;
                while (j < value.length() && value.charAt(j) != '}') {
                    ++j;
                }
                if (j > i) {
                    String envKey = value.substring(i + 1, j);
                    builder.append(System.getProperty(envKey));
                }
                i = j + 1;
            } else {
                builder.append(ch);
                ++i;
            }
        }
        return builder.toString();
    }
}

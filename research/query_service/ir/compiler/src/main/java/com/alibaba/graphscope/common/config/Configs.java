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

package com.alibaba.graphscope.common.config;

import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Configs {
    private Properties properties;

    public Configs(String file, FileLoadType loadType) throws IOException, NotImplementedException {
        properties = new Properties();
        switch (loadType) {
            case RELATIVE_PATH:
                properties.load(new FileInputStream(new File(file)));
                break;
            default:
                throw new NotImplementedException("unimplemented load type " + loadType);
        }
        // replace with the value from system property
        properties
                .keySet()
                .forEach(
                        k -> {
                            String value = System.getProperty((String) k);
                            String trimValue;
                            if (value != null && !(trimValue = value.trim()).isEmpty()) {
                                properties.setProperty((String) k, trimValue);
                            }
                        });
    }

    public Configs(Map<String, String> configs) {
        this.properties = new Properties();
        if (configs != null && !configs.isEmpty()) {
            configs.forEach(
                    (k, v) -> {
                        this.properties.setProperty(k, v);
                    });
        }
    }

    public String get(String name, String defaultValue) {
        return this.properties.getProperty(name, defaultValue);
    }
}

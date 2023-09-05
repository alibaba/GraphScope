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

import com.alibaba.graphscope.common.utils.FileUtils;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Configs {
    protected Properties properties;

    public Configs(String file) throws IOException {
        this(file, FileLoadType.RELATIVE_PATH);
    }

    public Configs(String file, FileLoadType loadType) throws IOException, NotImplementedException {
        properties = new Properties();
        switch (loadType) {
            case RELATIVE_PATH:
                properties.load(new FileInputStream(file));
                break;
            default:
                throw new NotImplementedException("unimplemented load type " + loadType);
        }
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

    public String get(String name) {
        String value;
        if ((value = System.getenv(name)) != null) {
            return value;
        } else if ((value = System.getProperty(name)) != null) {
            return value;
        } else {
            return this.properties.getProperty(name);
        }
    }

    public String get(String name, String defaultValue) {
        String value;
        if (!StringUtils.isEmpty(value = System.getenv(name))) {
            return value;
        } else if (!StringUtils.isEmpty(value = System.getProperty(name))) {
            return value;
        } else {
            return this.properties.getProperty(name, defaultValue);
        }
    }

    @Override
    public String toString() {
        return this.properties.toString();
    }

    public static class Factory {
        public static Configs create(String file) throws Exception {
            switch (FileUtils.getFormatType(file)) {
                case YAML:
                    return new YamlConfigs(file);
                case PROPERTIES:
                    return new Configs(file);
                case JSON:
                default:
                    throw new UnsupportedOperationException(
                            "can not initiate Configs from the file " + file);
            }
        }
    }
}

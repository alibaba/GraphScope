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
package com.alibaba.graphscope.gaia.plan.translator.builder;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

public class ConfigBuilder<T> {
    private Configuration conf = new BaseConfiguration();

    public T addConfig(String key, Object value) {
        if (this.conf.containsKey(key)) {
            this.conf.setProperty(key, value);
        } else {
            this.conf.addProperty(key, value);
        }
        return (T) this;
    }

    public Object getConfig(String key) {
        return this.conf.getProperty(key);
    }

    public T addAllConfigs(final Configuration conf) {
        this.conf.clear();
        Iterator<String> keys = conf.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            this.conf.addProperty(key, conf.getProperty(key));
        }
        return (T) this;
    }

    public final Configuration getConf() {
        return this.conf;
    }

    public T setConf(Configuration conf) {
        this.conf = conf;
        return (T) this;
    }
}

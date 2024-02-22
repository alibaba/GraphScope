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

package com.alibaba.graphscope.common.utils;

import com.alibaba.graphscope.common.ir.meta.reader.FileFormatType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.parser.ParserException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class FileUtils {
    public static String readJsonFromResource(String file) {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource(file);
            return Resources.toString(url, StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static FileFormatType getFormatType(String file) throws IOException {
        // cannot differentiate between properties and YAML format files based on their content,
        // so here the determination is made based on the file extension.
        if (file.endsWith(".properties")) return FileFormatType.PROPERTIES;
        try (InputStream inputStream = new FileInputStream(file)) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(inputStream);
            return FileFormatType.JSON;
        } catch (IOException e1) {
            try (InputStream inputStream = new FileInputStream(file)) {
                Yaml yaml = new Yaml();
                yaml.load(inputStream);
                return FileFormatType.YAML;
            } catch (ParserException e2) {
                throw new UnsupportedOperationException("unsupported file format " + file);
            }
        }
    }
}

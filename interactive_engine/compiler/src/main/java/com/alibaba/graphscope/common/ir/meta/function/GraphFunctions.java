/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.function;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.google.common.collect.Maps;

import org.apache.calcite.sql.type.GraphInferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class GraphFunctions {
    public static final String FUNCTION_PREFIX = "gs.function.";
    private final Map<String, FunctionMeta> functionMetaMap;

    private static GraphFunctions instance;

    public static GraphFunctions instance(Configs configs) {
        if (instance == null) {
            instance = new GraphFunctions(configs);
        }
        return instance;
    }

    private GraphFunctions(Configs configs) {
        this.functionMetaMap = Maps.newHashMap();
        this.registerBuiltInFunctions();
        this.registerConfigFunctions(configs);
    }

    // initialize built-in functions
    private void registerBuiltInFunctions() {
        for (BuiltInFunction function : BuiltInFunction.values()) {
            FunctionMeta meta = new FunctionMeta(function);
            functionMetaMap.put(function.getSignature(), meta);
        }
    }

    // initialize functions from configuration:
    // 1. load the functions from the settings of 'graph.functions'
    // 2. if the setting not found, load the default resource under the classpath
    private void registerConfigFunctions(Configs configs) {
        try (InputStream stream = loadConfigAsStream(configs)) {
            Yaml yaml = new Yaml();
            Map<String, Object> map = yaml.load(stream);
            if (map != null) {
                Object functions = map.get("graph_functions");
                if (functions instanceof List) {
                    for (Object function : (List) functions) {
                        String functionYaml = yaml.dump(function);
                        StoredProcedureMeta meta =
                                StoredProcedureMeta.Deserializer.perform(
                                        new ByteArrayInputStream(
                                                functionYaml.getBytes(StandardCharsets.UTF_8)));
                        functionMetaMap.put(meta.getName(), new FunctionMeta(meta));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream loadConfigAsStream(Configs configs) throws Exception {
        String functionConfig = GraphConfig.GRAPH_FUNCTIONS_URI.get(configs);
        if (!functionConfig.isEmpty()) {
            File file = new File(functionConfig);
            if (file.exists()) {
                return new FileInputStream(file);
            }
        }
        return Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("conf/graph_functions.yaml");
    }

    public FunctionMeta getFunction(String functionName) {
        FunctionMeta meta = functionMetaMap.get(functionName);
        if (meta == null) {
            // if not exist, create a new function meta with no constraints on operands and return
            // types
            meta =
                    new FunctionMeta(
                            functionName,
                            ReturnTypes.explicit(SqlTypeName.ANY),
                            OperandTypes.VARIADIC,
                            GraphInferTypes.FIRST_KNOWN);
        }
        return meta;
    }
}

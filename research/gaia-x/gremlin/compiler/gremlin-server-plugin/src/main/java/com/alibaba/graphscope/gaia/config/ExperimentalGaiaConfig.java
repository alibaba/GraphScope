package com.alibaba.graphscope.gaia.config;

import com.alibaba.graphscope.gaia.GlobalEngineConf;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class ExperimentalGaiaConfig implements GaiaConfig {
    public static String GAIA_ARGS_JSON = "gaia.args.json";
    public static String GRAPH_PROPERTIES = "graph.properties";
    public static String WORKERS = "workers";
    public static String HOSTS = "hosts";
    public static String TIME_LIMIT = "time_limit";
    public static String BATCH_SIZE = "batch_size";
    public static String OUT_CAPACITY = "output_capacity";
    public static String MEMORY_LIMIT = "memory_limit";
    public static String GREMLIN_GRAPH_SCHEMA = "gremlin.graph.schema";
    public static String GREMLIN_GRAPH_TYPE = "gremlin.graph.type";
    public static String OPTIMIZATIONS = "optimizations";

    private Map<String, Object> gaiaArgsJson;
    private Properties graphProperties;
    private Graph.Variables variables;

    public ExperimentalGaiaConfig(String confDir) {
        try {
            // init from file
            String configInJson = PlanUtils.readJsonFromFile(confDir + File.separator + GAIA_ARGS_JSON);
            this.gaiaArgsJson = JsonUtils.fromJson(configInJson, new TypeReference<Map<String, Object>>() {
            });
            this.graphProperties = new Properties();
            this.graphProperties.load(new FileInputStream(new File(confDir + File.separator + GRAPH_PROPERTIES)));
            for (String key : gaiaArgsJson.keySet()) {
                String sysVal = System.getProperty(key);
                if (sysVal != null) {
                    this.gaiaArgsJson.put(key, sysVal);
                }
            }
            for (Object key : graphProperties.keySet()) {
                String sysVal = System.getProperty((String) key);
                if (sysVal != null) {
                    this.graphProperties.setProperty((String) key, sysVal);
                }
            }
            variables = GlobalEngineConf.getGlobalVariables();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPegasusWorkerNum() {
        // reset from console
        if (variables != null && variables.get(WORKERS).isPresent()) {
            return (int) variables.get(WORKERS).get();
        } else {
            return (int) this.gaiaArgsJson.getOrDefault(WORKERS, DEFAULT_PEGASUS_WORKER_NUM);
        }
    }

    @Override
    public int getPegasusServerNum() {
        return getPegasusPhysicalHosts().size();
    }

    @Override
    public long getPegasusTimeout() {
        if (variables != null && variables.get(TIME_LIMIT).isPresent()) {
            return (long) variables.get(TIME_LIMIT).get();
        } else {
            return (Integer) this.gaiaArgsJson.getOrDefault(TIME_LIMIT, DEFAULT_PEGASUS_TIMEOUT);
        }
    }

    @Override
    public int getPegasusBatchSize() {
        return (int) this.gaiaArgsJson.getOrDefault(BATCH_SIZE, DEFAULT_PEGASUS_BATCH_SIZE);
    }

    @Override
    public int getPegasusOutputCapacity() {
        return (int) this.gaiaArgsJson.getOrDefault(OUT_CAPACITY, DEFAULT_PEGASUS_OUTPUT_CAPACITY);
    }

    @Override
    public int getPegasusMemoryLimit() {
        return (int) this.gaiaArgsJson.getOrDefault(MEMORY_LIMIT, DEFAULT_PEGASUS_MEMORY_LIMIT);
    }

    @Override
    public List<String> getPegasusPhysicalHosts() {
        return (List<String>) this.gaiaArgsJson.getOrDefault(HOSTS, Collections.EMPTY_LIST);
    }

    @Override
    public String getSchemaFilePath() {
        return this.graphProperties.getProperty(GREMLIN_GRAPH_SCHEMA, DEFAULT_SCHEMA_PATH);
    }

    @Override
    public GraphType getGraphType() {
        return GraphType.EXPERIMENTAL;
    }

    @Override
    public boolean getOptimizationStrategyFlag(String strategyFlagName) {
        String sysVal = System.getProperty(strategyFlagName);
        if (sysVal != null) {
            return Boolean.valueOf(sysVal);
        }
        if (variables != null && variables.get(strategyFlagName).isPresent()) {
            return (boolean) variables.get(strategyFlagName).get();
        }
        Map<String, Boolean> optFlag = (Map<String, Boolean>) this.gaiaArgsJson.get(OPTIMIZATIONS);
        if (optFlag == null) return DEFAULT_OPT_FLAG;
        return optFlag.getOrDefault(strategyFlagName, DEFAULT_OPT_FLAG);
    }
}

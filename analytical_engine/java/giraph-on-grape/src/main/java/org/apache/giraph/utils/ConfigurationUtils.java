/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.utils;

import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_CLASS;
import static org.apache.giraph.conf.GiraphConstants.EDGE_MANAGER;
import static org.apache.giraph.conf.GiraphConstants.TYPES_HOLDER_CLASS;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.fragment.IFragment;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationUtils {

    // These strings should be exactly the same as c++
    public static final String APP_CLASS_STR = "user_app_class";
    public static final String WORKER_CONTEXT_CLASS_STR = "worker_context_class";
    public static final String VERTEX_INPUT_FORMAT_CLASS_STR = "vertex_input_format_class";
    public static final String EDGE_INPUT_FORMAT_CLASS_STR = "edge_input_format_class";
    public static final String VERTEX_OUTPUT_FORMAT_CLASS_STR = "vertex_output_format_class";
    public static final String VERTEX_OUTPUT_FORMAT_SUBDIR_STR = "vertex_output_format_subdir";
    public static final String VERTEX_OUTPUT_PATH_STR = "vertex_output_path";
    public static final String MESSAGE_COMBINER_CLASS_STR = "message_combiner_class";
    public static final String MASTER_COMPUTE_CLASS_STR = "master_compute_class";
    public static final String EDGE_MANAGER_STR = "edge_manager";
    private static final Set<String> preservedKeysSet =
            new HashSet<>(
                    Arrays.asList(
                            APP_CLASS_STR,
                            WORKER_CONTEXT_CLASS_STR,
                            VERTEX_INPUT_FORMAT_CLASS_STR,
                            EDGE_INPUT_FORMAT_CLASS_STR,
                            VERTEX_OUTPUT_FORMAT_CLASS_STR,
                            VERTEX_OUTPUT_FORMAT_SUBDIR_STR,
                            VERTEX_OUTPUT_PATH_STR,
                            MESSAGE_COMBINER_CLASS_STR,
                            MASTER_COMPUTE_CLASS_STR,
                            EDGE_MANAGER_STR));
    private static Logger logger = LoggerFactory.getLogger(ConfigurationUtils.class);

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            logger.error("Empty class Path!");
            return new URL[] {};
        }
        String[] splited = classPath.split(":");
        List<URL> res =
                Arrays.stream(splited)
                        .map(File::new)
                        .map(
                                file -> {
                                    try {
                                        return file.toURL();
                                    } catch (MalformedURLException e) {
                                        e.printStackTrace();
                                    }
                                    return null;
                                })
                        .collect(Collectors.toList());
        logger.info(
                "Extracted URL"
                        + String.join(
                                ":", res.stream().map(URL::toString).collect(Collectors.toList())));
        URL[] ret = new URL[splited.length];
        for (int i = 0; i < splited.length; ++i) {
            ret[i] = res.get(i);
        }
        return ret;
    }

    /**
     * Translate CLI arguments to GiraphRunner into Configuration Key-Value pairs.
     *
     * @param giraphConfiguration configuration to set.
     * @param jsonObject          input json params
     */
    public static void parseArgs(
            final GiraphConfiguration giraphConfiguration, JSONObject jsonObject)
            throws ClassNotFoundException {
        // String user_jar_path = System.getenv("USER_JAR_PATH");
        // if (Objects.nonNull(user_jar_path) && !user_jar_path.isEmpty()){
        //     logger.info("user jar path: " + user_jar_path);
        //     giraphConfiguration.setClassLoader(new
        // URLClassLoader(classPath2URLArray(user_jar_path)));
        // }
        ClassLoader loader = giraphConfiguration.getClassLoader();

        if (jsonObject.containsKey(WORKER_CONTEXT_CLASS_STR)
                && !jsonObject.getString(WORKER_CONTEXT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setWorkerContextClass(
                    (Class<? extends WorkerContext>)
                            loader.loadClass(jsonObject.getString(WORKER_CONTEXT_CLASS_STR)));
            logger.info(
                    "Setting worker context class: "
                            + jsonObject.getString(WORKER_CONTEXT_CLASS_STR));
        } else {
            // set the default worker context class
            giraphConfiguration.setWorkerContextClass(DefaultWorkerContext.class);
            logger.info("Setting worker context class: " + DefaultWorkerContext.class.getName());
        }

        if (jsonObject.containsKey(APP_CLASS_STR)
                && !jsonObject.getString(APP_CLASS_STR).isEmpty()) {
            giraphConfiguration.setComputationClass(
                    (Class<? extends AbstractComputation>)
                            loader.loadClass(jsonObject.getString(APP_CLASS_STR)));
            logger.info("Setting app class: " + jsonObject.getString(APP_CLASS_STR));
        } else {
            logger.info("No computation class defined");
        }

        if (jsonObject.containsKey(VERTEX_INPUT_FORMAT_CLASS_STR)
                && !jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setVertexInputFormatClass(
                    (Class<? extends VertexInputFormat>)
                            loader.loadClass(
                                    trim(jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR))));
            logger.info(
                    "Setting vertex input format class: "
                            + jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR));
        } else {
            logger.error("No vertex input format class found");
        }

        if (jsonObject.containsKey(EDGE_INPUT_FORMAT_CLASS_STR)
                && !jsonObject.getString(EDGE_INPUT_FORMAT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setEdgeInputFormatClass(
                    (Class<? extends EdgeInputFormat>)
                            loader.loadClass(
                                    trim(jsonObject.getString(EDGE_INPUT_FORMAT_CLASS_STR))));
            logger.info(
                    "Setting edge input format class: "
                            + jsonObject.getString(EDGE_INPUT_FORMAT_CLASS_STR));
        } else {
            logger.info("No edge input format class found");
        }

        // Vertex output format
        if (jsonObject.containsKey(VERTEX_OUTPUT_FORMAT_CLASS_STR)
                && !jsonObject.getString(VERTEX_OUTPUT_FORMAT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setVertexOutputFormatClass(
                    (Class<? extends VertexOutputFormat>)
                            loader.loadClass(
                                    trim(jsonObject.getString(VERTEX_OUTPUT_FORMAT_CLASS_STR))));
            logger.info(
                    "Setting vertex input format class: "
                            + jsonObject.getString(VERTEX_OUTPUT_FORMAT_CLASS_STR));
        } else {
            logger.info("No vertex output format class found, using default one.");
        }

        // Vertex subdir
        if (jsonObject.containsKey(VERTEX_OUTPUT_FORMAT_SUBDIR_STR)
                && !jsonObject.getString(VERTEX_OUTPUT_FORMAT_SUBDIR_STR).isEmpty()) {
            giraphConfiguration.setVertexOutputFormatSubdir(
                    jsonObject.getString(VERTEX_OUTPUT_FORMAT_SUBDIR_STR));
            logger.info(
                    "Setting vertex output format subdir to "
                            + jsonObject.getString(VERTEX_OUTPUT_FORMAT_SUBDIR_STR));
        } else {
            logger.info("No vertex output format subdir specified, output to current dir");
        }
        // Vertex output path
        if (jsonObject.containsKey(VERTEX_OUTPUT_PATH_STR)
                && !jsonObject.getString(VERTEX_OUTPUT_PATH_STR).isEmpty()) {
            giraphConfiguration.setVertexOutputPath(jsonObject.getString(VERTEX_OUTPUT_PATH_STR));
            logger.info("Setting output path to: " + jsonObject.getString(VERTEX_OUTPUT_PATH_STR));
        }

        // For fast speed, we may just use the minimum combiner.
        if (jsonObject.containsKey(MESSAGE_COMBINER_CLASS_STR)
                && !jsonObject.getString(MESSAGE_COMBINER_CLASS_STR).isEmpty()) {
            giraphConfiguration.setMessageCombinerClass(
                    (Class<? extends MessageCombiner>)
                            loader.loadClass(jsonObject.getString(MESSAGE_COMBINER_CLASS_STR)));
            logger.info(
                    "Setting message combiner class to : "
                            + jsonObject.getString(MESSAGE_COMBINER_CLASS_STR));
        }

        // master compute class
        if (jsonObject.containsKey(MASTER_COMPUTE_CLASS_STR)
                && !jsonObject.getString(MASTER_COMPUTE_CLASS_STR).isEmpty()) {
            giraphConfiguration.setMasterComputeClass(
                    (Class<? extends MasterCompute>)
                            loader.loadClass(jsonObject.getString(MASTER_COMPUTE_CLASS_STR)));
            logger.info(
                    "Setting master compute class: "
                            + jsonObject.getString(MASTER_COMPUTE_CLASS_STR));
        }

        // Parse edge manager type
        if (jsonObject.containsKey(EDGE_MANAGER_STR)
                && !jsonObject.getString(EDGE_MANAGER_STR).isEmpty()) {
            String edgeManagerType = jsonObject.getString(EDGE_MANAGER_STR);
            giraphConfiguration.setEdgeManager(edgeManagerType);
            logger.info("Using edge manager of type [{}]", edgeManagerType);
        } else {
            logger.info("Using default message manager type", EDGE_MANAGER.getDefaultValue());
        }

        // for other user-defined keys, we put then in configuration.
        Set<String> keysSet = jsonObject.keySet();
        for (String str : keysSet) {
            if (!preservedKeysSet.contains(str)) {
                logger.info("Found user defined params: {}: {}", str, jsonObject.getString(str));
                giraphConfiguration.set(str, jsonObject.getString(str));
            }
        }
    }

    /**
     * Get a class which is parameterized by the graph types defined by user. The types holder is
     * actually an interface that any class which holds all of Giraph types can implement. It is
     * used with reflection to infer the Giraph types.
     *
     * <p>The current order of type holders we try are: 1) The {@link TypesHolder} class directly.
     * 2) The {@link org.apache.giraph.graph.Computation} class, as that holds all the types.
     *
     * @param conf Configuration
     * @return {@link TypesHolder} or null if could not find one.
     */
    public static Class<? extends TypesHolder> getTypesHolderClass(Configuration conf) {
        Class<? extends TypesHolder> klass = TYPES_HOLDER_CLASS.get(conf);
        if (klass != null) {
            return klass;
        }
        klass = COMPUTATION_CLASS.get(conf);
        return klass;
    }

    /**
     * For input IFragment, we check parse the type arguments, and set to giraphConfiguration.
     *
     * @param giraphConfiguration configuration to set.
     * @param fragment            IFragment obj.
     */
    public static void parseJavaFragment(
            final GiraphConfiguration giraphConfiguration, IFragment fragment) {}

    public static boolean checkTypeConsistency(
            Class<?> grapeTypeClass, Class<? extends Writable> giraphTypeClass) {
        if (grapeTypeClass.equals(Long.class)) {
            return giraphTypeClass.equals(LongWritable.class);
        }
        if (grapeTypeClass.equals(Integer.class)) {
            return giraphTypeClass.equals(IntWritable.class);
        }
        if (grapeTypeClass.equals(Double.class)) {
            return giraphTypeClass.equals(DoubleWritable.class);
        }
        if (grapeTypeClass.equals(Float.class)) {
            return giraphTypeClass.equals(FloatWritable.class);
        }
        logger.error(
                "Unsupported grape type and giraph type: "
                        + grapeTypeClass.getName()
                        + ", "
                        + giraphTypeClass.getName());
        return false;
    }

    /**
     * Configure an object with an {@link org.apache.giraph.conf.ImmutableClassesGiraphConfiguration}
     * if that objects supports it.
     *
     * @param object        The object to configure
     * @param configuration The configuration
     */
    public static void configureIfPossible(
            Object object, ImmutableClassesGiraphConfiguration configuration) {
        if (configuration != null) {
            configuration.configureIfPossible(object);
        } else if (object instanceof GiraphConfigurationSettable) {
            throw new IllegalArgumentException(
                    "Trying to configure configurable object without value, " + object.getClass());
        }
    }

    private static String trim(String str) {
        if (Objects.isNull(str) || str.isEmpty()) {
            throw new IllegalStateException("empty str");
        }
        if (str.startsWith("giraph:") || str.startsWith("Giraph:")) {
            return str.substring(7);
        }
        return str;
    }
}

/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.common.SchedulerEnvs;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.alibaba.maxgraph.common.cluster.MaxGraphConfiguration;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.alibaba.maxgraph.sdkcommon.util.PropertyUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";

    private static Map<String, String> extractConfig(Class configClass, MaxGraphConfiguration conf) {
        return MaxGraphConfiguration.getAllStaticMembers(configClass).stream()
                .filter(key -> conf.getOption(key).isPresent())
                .collect(Collectors.toMap(x -> x, conf::getString));
    }

    public static ThreadFactory createFactoryWithDefaultExceptionHandler(String serviceName, Logger LOG) {
        return (new ThreadFactoryBuilder()).setNameFormat(serviceName + "-%d").setDaemon(true).setUncaughtExceptionHandler((t, e) -> {
            LOG.error("exception in serviceName: " + serviceName + ", thread: " + t.getName());
        }).build();
    }

    public static void sleepSilently(long timeInMs) {
        try {
            Thread.sleep(timeInMs);
        } catch (InterruptedException ignored) {
        }
    }

    public static String exceptionToString(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static Map<String, String> getConfigFromSystemEnv() {
        String workerParameters = Stream.of(System.getenv(SchedulerEnvs.YARN_WORKER_ENV))
                .filter(org.apache.commons.lang3.StringUtils::isNotEmpty)
                .findFirst()
                .orElse(null);

        if (StringUtils.isEmpty(workerParameters)) {
            return Maps.newHashMap();
        } else {
            return JSON.fromJson(workerParameters.replace("\\", ""), new TypeReference<Map<String, String>>() {
            });
        }
    }

    public static InstanceConfig getInstanceConfig(String[] args, int serverId) throws IOException {
        Map<String, String> params = getConfigFromSystemEnv();
        if (params != null && !params.isEmpty()) {
            LOG.info("read configs from system env:{}", params);
            return new InstanceConfig(params);
        }

        InstanceConfig config;
        if (args != null && args.length > 0) {
            Properties properties = PropertyUtil.getProperties(args[0], false);
            config = new InstanceConfig(properties);
        } else {
            String confPath = new File("").getCanonicalPath() + "/interactive_engine/src/assembly/conf/standalone.properties";
            Properties properties = PropertyUtil.getProperties(confPath, false);
            config = new InstanceConfig(properties);
        }

        config.set(InstanceConfig.SERVER_ID, serverId);

        if (args != null && args.length > 2) {
            String uniqueGraphName = args[2];
            config.set(InstanceConfig.GRAPH_NAME, uniqueGraphName);
        }
        return config;
    }

    public static String getCurrentDate() {
        return new SimpleDateFormat(DATE_FORMAT).format(System.currentTimeMillis());
    }


    public static String getYarnLogDir() {
//        String yarnLogDir = System.getenv(ApplicationConstants.Environment.LOG_DIRS.name());
        String yarnLogDir = StringUtils.EMPTY;
        if (StringUtils.isEmpty(yarnLogDir)) {
            return "/tmp";
        } else {
            return yarnLogDir;
        }
    }

    public static String parseTimestamp(Long timestamp) {
        return new SimpleDateFormat(DATE_FORMAT).format(timestamp);
    }

    public static ExecutorService getGrpcExecutor(int threadCount) {
        return new ForkJoinPool(
                threadCount,
                new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                    final AtomicInteger num = new AtomicInteger();

                    @Override
                    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                        ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                        thread.setDaemon(true);
                        thread.setName("grpc-worker-" + "-" + num.getAndIncrement());
                        return thread;
                    }
                },
                (thread, e) -> {
                    LOG.error("Uncaught exception in thread: {}", thread.getName(), e);
                },
                true);
    }

    public static void wrappedException(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }
}

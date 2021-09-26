/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Set;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker context for random walks.
 */
public class RandomWalkWorkerContext extends WorkerContext {

    /**
     * Default maximum number of iterations
     */
    private static final int DEFAULT_MAX_SUPERSTEPS = 30;
    /**
     * Default teleportation probability
     */
    private static final float DEFAULT_TELEPORTATION_PROBABILITY = 0.15f;
    /**
     * Maximum number of iterations
     */
    private static int MAX_SUPERSTEPS;
    /**
     * Teleportation probability
     */
    private static double TELEPORTATION_PROBABILITY;
    /**
     * Preference vector
     */
    private static Set<Long> SOURCES;

    /**
     * Configuration parameter for the source vertex
     */
    private static final String SOURCE_VERTEX =
        RandomWalkWithRestartComputation.class.getName() + ".sourceVertex";

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory
        .getLogger(RandomWalkWorkerContext.class);

    /**
     * @return The maximum number of iterations to perform.
     */
    public int getMaxSupersteps() {
        if (MAX_SUPERSTEPS == 0) {
            throw new IllegalStateException(
                RandomWalkWorkerContext.class.getSimpleName() +
                    " was not initialized. Relaunch your job " +
                    "by setting the appropriate WorkerContext");
        }
        return MAX_SUPERSTEPS;
    }

    /**
     * @return The teleportation probability.
     */
    public double getTeleportationProbability() {
        if (TELEPORTATION_PROBABILITY == 0) {
            throw new IllegalStateException(
                RandomWalkWorkerContext.class.getSimpleName() +
                    " was not initialized. Relaunch your job " +
                    "by setting the appropriate WorkerContext");
        }
        return TELEPORTATION_PROBABILITY;
    }

    /**
     * Checks if a vertex is a source.
     *
     * @param id The vertex ID to check.
     * @return True if the vertex is a source in the preference vector.
     */
    public boolean isSource(long id) {
        return SOURCES.contains(id);
    }

    /**
     * @return The number of sources in the preference vector.
     */
    public int numSources() {
        return SOURCES.size();
    }

    /**
     * Initialize sources for Random Walk with Restart. First option (preferential) is single source
     * given from the command line as a parameter. Second option is a file with a list of vertex
     * IDs, one per line. In this second case the preference vector is a uniform distribution over
     * these vertexes.
     *
     * @param configuration The configuration.
     * @return a (possibly empty) set of source vertices
     */
    private ImmutableSet<Long> initializeSources(Configuration configuration) {
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        long sourceVertex = configuration.getLong(SOURCE_VERTEX, Long.MIN_VALUE);
        if (sourceVertex != Long.MIN_VALUE) {
            return ImmutableSet.of(sourceVertex);
        } else {
            Path sourceFile = null;
            try {

                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
                if (cacheFiles == null || cacheFiles.length == 0) {
                    // empty set if no source vertices configured
                    return ImmutableSet.of();
                }

                sourceFile = cacheFiles[0];
                FileSystem fs = FileSystem.getLocal(configuration);
                BufferedReader in = new BufferedReader(new InputStreamReader(
                    fs.open(sourceFile), Charset.defaultCharset()));
                String line;
                while ((line = in.readLine()) != null) {
                    builder.add(Long.parseLong(line));
                }
                in.close();
            } catch (IOException e) {
                getContext().setStatus(
                    "Could not load local cache files: " + sourceFile);
                LOG.error("Could not load local cache files: " + sourceFile, e);
            }
        }
        return builder.build();
    }

    @Override
    public void preApplication() throws InstantiationException,
        IllegalAccessException {
        setStaticVars(getContext().getConfiguration());
    }

    /**
     * Set static variables from Configuration
     *
     * @param configuration the conf
     */
    private void setStaticVars(Configuration configuration) {
        MAX_SUPERSTEPS = configuration.getInt(RandomWalkComputation.MAX_SUPERSTEPS,
            DEFAULT_MAX_SUPERSTEPS);
        TELEPORTATION_PROBABILITY = configuration.getFloat(
            RandomWalkComputation.TELEPORTATION_PROBABILITY,
            DEFAULT_TELEPORTATION_PROBABILITY);
        SOURCES = initializeSources(configuration);
    }

    @Override
    public void preSuperstep() {
    }

    @Override
    public void postSuperstep() {
    }

    @Override
    public void postApplication() {
    }
}

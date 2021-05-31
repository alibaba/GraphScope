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
package com.alibaba.maxgraph.compiler.step;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

public class MaxGraphIoStep<S> extends IoStep<S> {

    public MaxGraphIoStep(Traversal.Admin traversal, String file) {
        super(traversal, file);
    }

    @Override
    protected Traverser.Admin<S> read(final File file) {
        try (final InputStream stream = new FileInputStream(file)) {
            final Graph graph = (Graph) this.traversal.getGraph().get();
            constructReader().readGraph(stream, graph);

            return EmptyTraverser.instance();
        } catch (IOException ioe) {
            throw new IllegalStateException(String.format("Could not read file %s into graph", this.getFile()), ioe);
        }
    }

    /**
     * Builds a {@link GraphReader} instance to use. Attempts to detect the file format to be read using the file
     * extension or simply uses configurations provided by the user on the parameters given to the step.
     */
    private GraphReader constructReader() {
        final Object objectOrClass = super.getParameters().get(IO.reader, this::detectFileType).get(0);
        if (objectOrClass instanceof GraphReader)
            return (GraphReader) objectOrClass;
        else if (objectOrClass instanceof String) {
            if (objectOrClass.equals(IO.graphson)) {
                final GraphSONMapper.Builder builder = GraphSONMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return GraphSONReader.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.gryo)) {
                final GryoMapper.Builder builder = GryoMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return MaxGraphGryoReader.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.graphml))
                return GraphMLReader.build().create();
            else {
                try {
                    final Class<?> graphReaderClazz = Class.forName((String) objectOrClass);
                    final Method build = graphReaderClazz.getMethod("build");
                    final GraphReader.ReaderBuilder builder = (GraphReader.ReaderBuilder) build.invoke(null);
                    return builder.create();
                } catch (Exception ex) {
                    throw new IllegalStateException(String.format("Could not construct the specified GraphReader of %s", objectOrClass), ex);
                }
            }
        } else {
            throw new IllegalStateException("GraphReader could not be determined");
        }
    }
}

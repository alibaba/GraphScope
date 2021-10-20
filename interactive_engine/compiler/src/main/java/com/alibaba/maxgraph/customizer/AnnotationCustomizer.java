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
package com.alibaba.maxgraph.customizer;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer;
import org.codehaus.groovy.ast.AnnotatedNode;
import org.codehaus.groovy.ast.ClassCodeVisitorSupport;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.syntax.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright ©Alibaba Inc.
 *
 * @author: ruoang
 * @date: 2019-08-12
 * @time: 18:25
 */
public class AnnotationCustomizer implements GroovyCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationCustomizer.class);

    @Override
    public CompilationCustomizer create() {
        return new CompilationCustomizer(CompilePhase.CONVERSION) {
            @Override
            public void call(SourceUnit source, GeneratorContext context, ClassNode classNode)
                throws CompilationFailedException {
                classNode.visitContents(new ClassCodeVisitorSupport() {
                    @Override
                    protected SourceUnit getSourceUnit() {
                        return source;
                    }

                    @Override
                    public void visitAnnotations(AnnotatedNode node) {
                        if (!node.getAnnotations().isEmpty()) {
                            LOG.error("annotation: " + (source.getSource()));
                            source.addError(new SyntaxException("annotation is disable", classNode.getLineNumber(),
                                classNode.getColumnNumber()));
                        }
                    }
                });
            }
        };
    }
}

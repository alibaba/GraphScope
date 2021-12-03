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

package com.alibaba.graphscope.annotation;

import static com.alibaba.graphscope.annotation.Utils.addCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.addDoubleCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.addIntCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.addLongCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.addSignedIntCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.addSignedLongCXXTemplate;
import static com.alibaba.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_COLUMN;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;
import static com.alibaba.graphscope.utils.CppClassName.INT_COLUMN;
import static com.alibaba.graphscope.utils.CppClassName.LONG_COLUMN;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIApplication;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGenBatch;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIMirror;
import com.alibaba.fastffi.FFIMirrorDefinition;
import com.alibaba.fastffi.FFIMirrorFieldDefinition;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFIType;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.fastffi.annotation.AnnotationProcessorUtils;
import com.alibaba.graphscope.column.DoubleColumn;
import com.alibaba.graphscope.column.IntColumn;
import com.alibaba.graphscope.column.LongColumn;
import com.alibaba.graphscope.context.ffi.FFILabeledVertexDataContext;
import com.alibaba.graphscope.context.ffi.FFILabeledVertexPropertyContext;
import com.alibaba.graphscope.context.ffi.FFIVertexDataContext;
import com.alibaba.graphscope.context.ffi.FFIVertexPropertyContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.GrapeAdjList;
import com.alibaba.graphscope.ds.GrapeNbr;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.PropertyAdjList;
import com.alibaba.graphscope.ds.PropertyNbr;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.PropertyMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SupportedAnnotationTypes({
    "com.alibaba.fastffi.FFIMirror",
    "com.alibaba.fastffi.FFIMirrorDefinition",
    "com.alibaba.graphscope.annotation.GraphType"
})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GraphScopeAnnotationProcessor extends javax.annotation.processing.AbstractProcessor {
    public static final String GraphTypeWrapperSuffix = "$$GraphTypeDefinitions";
    static final String JNI_HEADER = "<jni.h>";
    private static Logger logger =
            LoggerFactory.getLogger(GraphScopeAnnotationProcessor.class.getName());
    Map<String, String> foreignTypeNameToJavaTypeName = new HashMap<>();
    Map<String, FFIMirrorDefinition> foreignTypeNameToFFIMirrorDefinitionMap = new HashMap<>();
    Map<String, FFIMirrorDefinition> javaTypeNameToFFIMirrorDefinitionMap = new HashMap<>();
    String graphTypeElementName;
    boolean graphTypeWrapperGenerated = false;
    AnnotationMirror graphType = null;
    private String javaImmutableFragmentTemplateName = ImmutableEdgecutFragment.class.getName();
    private String foreignImmutableFragmentTemplateName = CppClassName.GRAPE_IMMUTABLE_FRAGMENT;

    private String javaArrowFragmentTemplateName = ArrowFragment.class.getName();
    private String foreignArrowFragmentTemplateName = ARROW_FRAGMENT;
    private String JavaArrowProjectedTemplateName = ArrowProjectedFragment.class.getName();
    private String foreignArrowProjectedTempalteName = ARROW_PROJECTED_FRAGMENT;

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            output();
            return false;
        }
        for (TypeElement annotation : annotations) {
            for (Element e : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (e instanceof TypeElement) {
                    processAnnotation((TypeElement) e, annotation);
                }
            }
        }
        return false;
    }

    public List<String> getMessageTypes() {
        List<String> messageTypes = new ArrayList<>();
        String messageTypeString = System.getProperty("grape.messageTypes");
        if (messageTypeString != null) {
            Arrays.asList(parseMessageTypes(messageTypeString)).forEach(p -> messageTypes.add(p));
        }
        return messageTypes;
    }

    /**
     * Use : to separate types
     *
     * @param messageTypes
     * @return
     */
    private String[] parseMessageTypes(String messageTypes) {
        String[] results =
                Arrays.stream(messageTypes.split(",")).map(m -> m.trim()).toArray(String[]::new);
        return results;
    }

    /**
     * Given one FFIMirror class name, we need to get the foreign type too. Currently don't support
     * primitive types if message is ffi, we use the foreign type.
     *
     * @param messageType message types concatenated by ":"
     * @return String [0] cxx type, String [1] java type
     */
    public String[] parseMessageType(String messageType) {
        return messageType.split("=");
    }

    boolean isGraphType(AnnotationMirror annotationMirror) {
        DeclaredType declaredType = annotationMirror.getAnnotationType();
        TypeMirror graphTypeAnnotationType =
                processingEnv.getElementUtils().getTypeElement(GraphType.class.getName()).asType();
        return processingEnv.getTypeUtils().isSameType(declaredType, graphTypeAnnotationType);
    }

    void assertNull(Object obj) {
        if (obj != null) {
            throw new IllegalArgumentException("Require null, got " + obj);
        }
    }

    void assertNullOrEquals(Object obj, Object expected) {
        if (obj != null && !obj.equals(expected)) {
            throw new IllegalArgumentException("Require null or " + expected + ", got " + obj);
        }
    }

    void processAnnotation(TypeElement typeElement, TypeElement annotation) {
        if (isSameType(annotation.asType(), FFIMirrorDefinition.class)) {
            FFIMirrorDefinition mirrorDefinition =
                    typeElement.getAnnotation(FFIMirrorDefinition.class);
            if (mirrorDefinition != null) {
                String foreignType = getFFIMirrorForeignTypeName(mirrorDefinition);
                String javaType = getFFIMirrorJavaTypeName(typeElement);
                assertNull(
                        foreignTypeNameToFFIMirrorDefinitionMap.put(foreignType, mirrorDefinition));
                assertNull(javaTypeNameToFFIMirrorDefinitionMap.put(javaType, mirrorDefinition));
                assertNullOrEquals(
                        foreignTypeNameToJavaTypeName.put(foreignType, javaType), javaType);
                checkAndGenerateGraphTypeWrapper();
            }
            return;
        }

        if (isSameType(annotation.asType(), GraphType.class)) {
            List<? extends AnnotationMirror> mirrors = typeElement.getAnnotationMirrors();
            for (AnnotationMirror annotationMirror : mirrors) {
                if (isGraphType(annotationMirror)) {
                    if (this.graphType != null) {
                        throw new IllegalStateException(
                                "Oops: An project can only have one @GraphType, "
                                        + "already have "
                                        + this.graphType
                                        + ", but got "
                                        + annotationMirror);
                    }
                    this.graphType = annotationMirror;
                    this.graphTypeElementName = typeElement.getQualifiedName().toString();
                    checkAndGenerateGraphTypeWrapper();
                }
            }
            return;
        }

        if (isSameType(annotation.asType(), FFIMirror.class)) {
            String foreignType = getForeignTypeNameByFFITypeAlias(typeElement);
            String javaType = typeElement.getQualifiedName().toString();
            String check = foreignTypeNameToJavaTypeName.put(foreignType, javaType);
            if (check != null) {
                processingEnv
                        .getMessager()
                        .printMessage(
                                Diagnostic.Kind.ERROR,
                                "Duplicalte FFIMirror on "
                                        + foreignType
                                        + ", expected "
                                        + check
                                        + ", got "
                                        + javaType);
            }
            return;
        }
    }

    /**
     * @param typeElement: the generated implementation of the FFIMirror
     * @return
     */
    private String getFFIMirrorJavaTypeName(TypeElement typeElement) {
        List<? extends TypeMirror> interfaces = typeElement.getInterfaces();
        if (interfaces.size() != 1) {
            throw new IllegalArgumentException(
                    "A generated class must only have one super interface: " + typeElement);
        }
        return AnnotationProcessorUtils.typeToTypeName(interfaces.get(0));
    }

    /**
     * Return the package element of the type element.
     *
     * @param typeElement
     * @return
     */
    private PackageElement getPackageElement(TypeElement typeElement) {
        return AnnotationProcessorUtils.getPackageElement(typeElement);
    }

    private void checkAndGenerateGraphTypeWrapper() {
        if (graphTypeWrapperGenerated == true) {
            return;
        }
        if (!checkPrecondition()) {
            return;
        }
        generateGraphTypeWrapper();
    }

    /**
     * For graphScope we don't generate immutableEdgecutFragment
     *
     * @param classBuilder
     */
    private void addGrapeWrapper(TypeSpec.Builder classBuilder) {
        DeclaredType edataType = getEdataType();
        // String foreignEdataType = getForeignTypeName(edataType);
        String javaEdataType = getTypeName(edataType);
        DeclaredType oidType = getOidType();
        // String foreignOidType = getForeignTypeName(oidType);
        String javaOidType = getTypeName(oidType);
        DeclaredType vdataType = getVdataType();
        // String foreignVdataType = getForeignTypeName(vdataType);
        String javaVdataType = getTypeName(vdataType);
        String foreignVidType = getGraphTypeMemberForeign("cppVidType");
        String foreignOidType = getGraphTypeMemberForeign("cppOidType");
        String foreignVdataType = getGraphTypeMemberForeign("cppVdataType");
        String foreignEdataType = getGraphTypeMemberForeign("cppEdataType");
        String foreignFragType = getGraphTypeMemberForeign("fragType");
        logger.info("foreign vid type " + foreignVidType);
        logger.info("foreign vdata type " + foreignVdataType);
        logger.info("foreign oid type " + foreignOidType);
        logger.info("foreign edata type " + foreignEdataType);
        logger.info("foriegn frag type :" + foreignFragType);
        String foreignArrowProjectFragNameConcat =
                makeParameterizedType(
                        foreignArrowProjectedTempalteName,
                        foreignOidType,
                        "uint64_t",
                        foreignVdataType,
                        foreignEdataType);
        String javaArrowProjectFragNameConcat =
                makeParameterizedType(
                        JavaArrowProjectedTemplateName,
                        javaOidType,
                        "java.lang.Long",
                        javaVdataType,
                        javaEdataType);
        String foreignArrowFragNameConcat =
                makeParameterizedType(foreignArrowFragmentTemplateName, foreignOidType);
        String javaArrowFragNameConcat =
                makeParameterizedType(javaArrowFragmentTemplateName, javaOidType);
        logger.info(
                "fragment "
                        + foreignArrowProjectFragNameConcat
                        + ", "
                        + javaArrowProjectFragNameConcat);

        AnnotationSpec.Builder ffiGenBatchBuilder = AnnotationSpec.builder(FFIGenBatch.class);

        addVertexGen(ffiGenBatchBuilder);
        addVertexRange(ffiGenBatchBuilder);
        addGSVertexRange(ffiGenBatchBuilder);
        addNbr(ffiGenBatchBuilder);
        addAdjList(ffiGenBatchBuilder);

        addMessageTypes(ffiGenBatchBuilder);

        addGSVertexArray(ffiGenBatchBuilder);
        addVertexArray(ffiGenBatchBuilder);
        addStdVector(ffiGenBatchBuilder);

        if (foreignFragType.equals("vineyard::ArrowFragment")) {
            logger.info("Codegen for arrowFragment");
            {
                addPropertyNbr(ffiGenBatchBuilder);
                addPropertyAdjList(ffiGenBatchBuilder);
                addPropertyNbrUnit(ffiGenBatchBuilder);
                addPropertyRawAdjList(ffiGenBatchBuilder);
                addArrowFragment(ffiGenBatchBuilder);
                // add column
                addColumn(ffiGenBatchBuilder, foreignArrowFragNameConcat, javaArrowFragNameConcat);
                addSharedPtr(
                        ffiGenBatchBuilder, foreignArrowFragNameConcat, javaArrowFragNameConcat);
                // add context
                addLabeledVertexDataContext(
                        ffiGenBatchBuilder, foreignArrowFragNameConcat, javaArrowFragNameConcat);
                addLabeledVertexPropertyContext(
                        ffiGenBatchBuilder, foreignArrowFragNameConcat, javaArrowFragNameConcat);

                // PropertyMessageManager
                AnnotationSpec.Builder ffiGenBuildPropertyMM = AnnotationSpec.builder(FFIGen.class);
                ffiGenBuildPropertyMM.addMember(
                        "type", "$S", PropertyMessageManager.class.getName());
                propertyMMAddMessages(ffiGenBuildPropertyMM);
                ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuildPropertyMM.build());
            }
        } else if (foreignFragType.equals("gs::ArrowProjectedFragment")) {
            logger.info("Codegen for arrowProjectedFragment");

            addProjectedNbr(ffiGenBatchBuilder);
            addProjectedAdjList(ffiGenBatchBuilder);
            // generate for ArrowProjectedFragment
            addArrowProjectedFragment(
                    ffiGenBatchBuilder,
                    foreignOidType,
                    "uint64_t",
                    foreignVdataType,
                    foreignEdataType,
                    javaOidType,
                    Long.class.getName(),
                    javaVdataType,
                    javaEdataType);

            addDefaultMessageManager(
                    ffiGenBatchBuilder,
                    foreignArrowProjectFragNameConcat,
                    javaArrowProjectFragNameConcat);

            // add for all columns, long column, int column, double column.
            addColumn(
                    ffiGenBatchBuilder,
                    foreignArrowProjectFragNameConcat,
                    javaArrowProjectFragNameConcat);
            addSharedPtr(
                    ffiGenBatchBuilder,
                    foreignArrowProjectFragNameConcat,
                    javaArrowProjectFragNameConcat);

            addVertexDataContext(
                    ffiGenBatchBuilder,
                    foreignArrowProjectFragNameConcat,
                    javaArrowProjectFragNameConcat);
            addVertexPropertyContext(
                    ffiGenBatchBuilder,
                    foreignArrowProjectFragNameConcat,
                    javaArrowProjectFragNameConcat);

        } else {
            logger.error("Unrecoginizable");
        }
        classBuilder.addAnnotation(ffiGenBatchBuilder.build());
    }

    private void addVertexGen(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", Vertex.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addVertexRange(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", VertexRange.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addGSVertexRange(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", GSVertexArray.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addNbr(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", GrapeNbr.class.getName());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint32_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Integer")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addAdjList(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", GrapeAdjList.class.getName());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint32_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Integer")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addPropertyNbrUnit(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", PropertyNbrUnit.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addPropertyRawAdjList(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", PropertyRawAdjList.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addPropertyNbr(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", PropertyNbr.class.getName());
        addIntCXXTemplate(ffiGenVertex);
        addLongCXXTemplate(ffiGenVertex);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addPropertyAdjList(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", PropertyAdjList.class.getName());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint32_t")
                        .addMember("java", "$S", "Integer")
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("java", "$S", "Long")
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addProjectedNbr(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", ProjectedNbr.class.getName());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "int64_t")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Long")
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addProjectedAdjList(AnnotationSpec.Builder ffiGenBatchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", ProjectedAdjList.class.getName());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Double")
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("cxx", "$S", "int64_t")
                        .addMember("java", "$S", "Long")
                        .addMember("java", "$S", "Long")
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addMessageTypes(AnnotationSpec.Builder batchBuilder) {
        AnnotationSpec.Builder doubleMsgBuilder = AnnotationSpec.builder(FFIGen.class);
        doubleMsgBuilder.addMember("type", "$S", DoubleMsg.class.getName());
        batchBuilder.addMember("value", "$L", doubleMsgBuilder.build());
        AnnotationSpec.Builder longMsgBuilder = AnnotationSpec.builder(FFIGen.class);
        longMsgBuilder.addMember("type", "$S", DoubleMsg.class.getName());
        batchBuilder.addMember("value", "$L", longMsgBuilder.build());
    }

    private void addArrowFragment(AnnotationSpec.Builder batchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", ArrowFragment.class.getName());
        addSignedLongCXXTemplate(ffiGenVertex);
        addSignedIntCXXTemplate(ffiGenVertex);
        batchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addArrowProjectedFragment(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignOidType,
            String foreignVidType,
            String foreignVdataType,
            String foreignEdataType,
            String javaOidType,
            String javaVidType,
            String javaVdataType,
            String javaEdataType) {
        AnnotationSpec.Builder arrowProjectedFragmentBuilder = AnnotationSpec.builder(FFIGen.class);
        arrowProjectedFragmentBuilder
                .addMember("type", "$S", JavaArrowProjectedTemplateName)
                .addMember(
                        "templates",
                        "$L",
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignOidType)
                                .addMember("cxx", "$S", foreignVidType)
                                .addMember("cxx", "$S", foreignVdataType)
                                .addMember("cxx", "$S", foreignEdataType)
                                .addMember("java", "$S", javaOidType)
                                .addMember("java", "$S", javaVidType)
                                .addMember("java", "$S", javaVdataType)
                                .addMember("java", "$S", javaEdataType)
                                .build());
        ffiGenBatchBuilder.addMember("value", "$L", arrowProjectedFragmentBuilder.build());
    }

    private void addGSVertexArray(AnnotationSpec.Builder batchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", GSVertexArray.class.getName());
        addLongCXXTemplate(ffiGenVertex);
        addSignedLongCXXTemplate(ffiGenVertex);
        addIntCXXTemplate(ffiGenVertex);
        addSignedIntCXXTemplate(ffiGenVertex);
        addDoubleCXXTemplate(ffiGenVertex);
        batchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addVertexArray(AnnotationSpec.Builder batchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", VertexArray.class.getName());
        addCXXTemplate(ffiGenVertex, "double", "uint64_t", "Double", "Long");
        addCXXTemplate(ffiGenVertex, "int64_t", "uint64_t", "Long", "Long");
        addCXXTemplate(ffiGenVertex, "int32_t", "uint64_t", "Integer", "Long");
        batchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addStdVector(AnnotationSpec.Builder batchBuilder) {
        AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertex.addMember("type", "$S", StdVector.class.getName());
        addSignedLongCXXTemplate(ffiGenVertex);
        addSignedIntCXXTemplate(ffiGenVertex);
        // add support for complex gs vertex array
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", makeParameterizedType(GS_VERTEX_ARRAY, "double"))
                        .addMember(
                                "java",
                                "$S",
                                makeParameterizedType(GSVertexArray.class.getName(), "Double"))
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", makeParameterizedType(GS_VERTEX_ARRAY, "int64_t"))
                        .addMember(
                                "java",
                                "$S",
                                makeParameterizedType(GSVertexArray.class.getName(), "Long"))
                        .build());
        ffiGenVertex.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", makeParameterizedType(GS_VERTEX_ARRAY, "int32_t"))
                        .addMember(
                                "java",
                                "$S",
                                makeParameterizedType(GSVertexArray.class.getName(), "Integer"))
                        .build());

        batchBuilder.addMember("value", "$L", ffiGenVertex.build());
    }

    private void addColumn(
            AnnotationSpec.Builder batchBuilder, String foreignFragName, String javaFragName) {
        for (String columnName :
                new String[] {
                    DoubleColumn.class.getName(),
                    LongColumn.class.getName(),
                    IntColumn.class.getName()
                }) {
            AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
            ffiGenVertex.addMember("type", "$S", columnName);
            ffiGenVertex.addMember(
                    "templates",
                    "$L",
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", foreignFragName)
                            .addMember("java", "$S", javaFragName)
                            .build());
            batchBuilder.addMember("value", "$L", ffiGenVertex.build());
        }
    }

    private void addSharedPtr(
            AnnotationSpec.Builder batchBuilder, String foreignFragName, String javaFragName) {
        {
            AnnotationSpec.Builder ffiGenSharedPtr = AnnotationSpec.builder(FFIGen.class);
            ffiGenSharedPtr.addMember("type", "$S", StdSharedPtr.class.getName());
            for (String[] columnTypePair :
                    new String[][] {
                        {DOUBLE_COLUMN, DoubleColumn.class.getName()},
                        {INT_COLUMN, IntColumn.class.getName()},
                        {LONG_COLUMN, LongColumn.class.getName()}
                    }) {
                ffiGenSharedPtr.addMember(
                        "templates",
                        "$L",
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(columnTypePair[0], foreignFragName))
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(columnTypePair[1], javaFragName))
                                .build());
            }
            batchBuilder.addMember("value", "$L", ffiGenSharedPtr.build());
        }
    }

    private void addDefaultMessageManager(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenBuilderdefault = AnnotationSpec.builder(FFIGen.class);
        ffiGenBuilderdefault.addMember("type", "$S", DefaultMessageManager.class.getName());
        defaultMessageManagerAddMessages(ffiGenBuilderdefault, foreignFragName, javaFragName);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderdefault.build());
    }

    private void addVertexDataContext(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenVertexDataContext = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertexDataContext.addMember("type", "$S", FFIVertexDataContext.class.getName());
        vertexDataContextAddTemplate(ffiGenVertexDataContext, foreignFragName, javaFragName);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertexDataContext.build());
    }

    private void addVertexPropertyContext(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenVertexPropertyContext = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertexPropertyContext.addMember(
                "type", "$S", FFIVertexPropertyContext.class.getName());
        vertexDataContextAddTemplate(ffiGenVertexPropertyContext, foreignFragName, javaFragName);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertexPropertyContext.build());
    }

    private void addLabeledVertexDataContext(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenLabeledVertexDataContext =
                AnnotationSpec.builder(FFIGen.class);
        ffiGenLabeledVertexDataContext.addMember(
                "type", "$S", FFILabeledVertexDataContext.class.getName());
        vertexDataContextAddTemplate(ffiGenLabeledVertexDataContext, foreignFragName, javaFragName);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenLabeledVertexDataContext.build());
    }

    private void addLabeledVertexPropertyContext(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenLabeledVertexPropertyContext =
                AnnotationSpec.builder(FFIGen.class);
        ffiGenLabeledVertexPropertyContext.addMember(
                "type", "$S", FFILabeledVertexPropertyContext.class.getName());
        vertexPropertyContextAddTemplate(
                ffiGenLabeledVertexPropertyContext, foreignFragName, javaFragName);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenLabeledVertexPropertyContext.build());
    }

    private void defaultMessageManagerAddMessages(
            AnnotationSpec.Builder defaultMessagerBuilder,
            String foreignArrowProjectedTempalteNameConcat,
            String javaArrowProjectedTemplateNameConcat) {
        List<String> messageTypes = getMessageTypes();
        logger.info(
                "In DefaultMessageManager, received message types are: "
                        + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        {
            // sendMsgThroughIEdges
            AnnotationSpec.Builder sendIEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughIEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignArrowProjectedTempalteNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaArrowProjectedTemplateNameConcat)
                                .addMember("java", "$S", types[1]);
                sendIEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            defaultMessagerBuilder.addMember("functionTemplates", "$L", sendIEdgesBuilder.build());
        }
        {
            // sendMsgThroughOEdges
            AnnotationSpec.Builder sendOEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughOEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignArrowProjectedTempalteNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaArrowProjectedTemplateNameConcat)
                                .addMember("java", "$S", types[1]);
                sendOEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            defaultMessagerBuilder.addMember("functionTemplates", "$L", sendOEdgesBuilder.build());
        }
        {
            // sendMsgThroughEdges
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignArrowProjectedTempalteNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaArrowProjectedTemplateNameConcat)
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            defaultMessagerBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
        {
            // syncStateOnOuterVertex
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "syncStateOnOuterVertex")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignArrowProjectedTempalteNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaArrowProjectedTemplateNameConcat)
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            defaultMessagerBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
        {
            // getMessage
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "getMessage")
                            .addMember("returnType", "$S", "boolean")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", foreignArrowProjectedTempalteNameConcat)
                                .addMember("cxx", "$S", types[0])
                                .addMember("java", "$S", javaArrowProjectedTemplateNameConcat)
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            defaultMessagerBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
    }

    private void propertyMMAddMessages(AnnotationSpec.Builder propertyMMBuilder) {
        List<String> messageTypes = getMessageTypes();
        logger.info(
                "In propertyMessageManager, received message types are: "
                        + String.join(",", messageTypes));
        if (messageTypes.isEmpty()) {
            return;
        }
        {
            // sendMsgThroughIEdges
            AnnotationSpec.Builder sendIEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughIEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(
                                                javaArrowFragmentTemplateName,
                                                Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendIEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendIEdgesBuilder.build());
        }
        {
            // sendMsgThroughOEdges
            AnnotationSpec.Builder sendOEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughOEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(
                                                javaArrowFragmentTemplateName,
                                                Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendOEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendOEdgesBuilder.build());
        }
        {
            // sendMsgThroughEdges
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "sendMsgThroughEdges")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(
                                                javaArrowFragmentTemplateName,
                                                Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
        {
            // syncStateOnOuterVertex
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "syncStateOnOuterVertex")
                            .addMember("returnType", "$S", "void")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(
                                                javaArrowFragmentTemplateName,
                                                Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
        {
            // getMessage
            AnnotationSpec.Builder sendEdgesBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "getMessage")
                            .addMember("returnType", "$S", "boolean")
                            .addMember("parameterTypes", "$S", "FRAG_T")
                            .addMember("parameterTypes", "$S", "MSG_T");
            for (String messageType : messageTypes) {
                String[] types = parseMessageType(messageType);
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(ARROW_FRAGMENT, "int64_t"))
                                .addMember("cxx", "$S", types[0])
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(
                                                javaArrowFragmentTemplateName,
                                                Long.class.getName()))
                                .addMember("java", "$S", types[1]);
                sendEdgesBuilder.addMember("templates", "$L", templateBuilder.build());
            }
            propertyMMBuilder.addMember("functionTemplates", "$L", sendEdgesBuilder.build());
        }
    }

    private void vertexDataContextAddTemplate(
            AnnotationSpec.Builder vertexDataContextBuilder,
            String foreignFragName,
            String javaFragName) {
        for (String[] dataType :
                new String[][] {
                    {"int64_t", Long.class.getName()},
                    {"int32_t", Integer.class.getName()},
                    {"double", Double.class.getName()}
                }) {
            vertexDataContextBuilder.addMember(
                    "templates",
                    "$L",
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", foreignFragName)
                            .addMember("cxx", "$S", dataType[0])
                            .addMember("java", "$S", javaFragName)
                            .addMember("java", "$S", dataType[1])
                            .build());
        }
    }

    private void vertexPropertyContextAddTemplate(
            AnnotationSpec.Builder vertexPropertyContextBuilder,
            String foreignFragName,
            String javaFragName) {
        vertexPropertyContextBuilder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", foreignFragName)
                        .addMember("java", "$S", javaFragName)
                        .build());
    }

    private String makeParameterizedType(String base, String... types) {
        if (types.length == 0) {
            return base;
        }
        return base + "<" + String.join(",", types) + ">";
    }

    private void generateGrapeWrapper() {
        Random random = new Random();
        String packageName = "grape" + random.nextInt(Integer.MAX_VALUE);
        String classSimpleName = "GrapeWrapper" + random.nextInt(Integer.MAX_VALUE);
        TypeSpec.Builder classBuilder =
                TypeSpec.interfaceBuilder(classSimpleName).addModifiers(Modifier.PUBLIC);
        addGrapeWrapper(classBuilder);
        writeTypeSpec(packageName, classBuilder.build());
    }

    private void generateGraphTypeWrapper() {
        TypeElement typeElement =
                processingEnv.getElementUtils().getTypeElement(graphTypeElementName);
        String name = typeElement.getSimpleName().toString() + GraphTypeWrapperSuffix;
        String libraryName = null;
        PackageElement packageElement = getPackageElement(typeElement);
        FFIApplication application = packageElement.getAnnotation(FFIApplication.class);
        if (application != null) {
            libraryName = application.jniLibrary();
            // throw new IllegalStateException("The class annotated by @GraphType must be in package
            // with
            // a package-info.java and the package info must be annotated with FFIApplication.");
        }

        TypeSpec.Builder classBuilder =
                TypeSpec.interfaceBuilder(name).addModifiers(Modifier.PUBLIC);
        classBuilder.addSuperinterface(getTypeMirror(FFIPointer.class.getName()));
        if (libraryName == null || libraryName.isEmpty()) {
            classBuilder.addAnnotation(FFIGen.class);
        } else {
            classBuilder.addAnnotation(
                    AnnotationSpec.builder(FFIGen.class)
                            .addMember("value", "$S", libraryName)
                            .build());
        }
        classBuilder.addAnnotation(FFIMirror.class);
        classBuilder.addAnnotation(
                AnnotationSpec.builder(FFITypeAlias.class).addMember("value", "$S", name).build());

        Set<String> headers = getGraphTypeHeaders();
        for (String header : headers) {
            if (header.length() <= 2) {
                throw new IllegalStateException("Invalid header: " + header);
            }
            String internal = header.substring(1, header.length() - 1);
            if (header.charAt(0) == '<') {
                classBuilder.addAnnotation(
                        AnnotationSpec.builder(CXXHead.class)
                                .addMember("system", "$S", internal)
                                .build());
            } else {
                classBuilder.addAnnotation(
                        AnnotationSpec.builder(CXXHead.class)
                                .addMember("value", "$S", internal)
                                .build());
            }
        }

        addGraphTypeWrapper(classBuilder, "vidType", getVidType());
        addGraphTypeWrapper(classBuilder, "edataType", getEdataType());
        addGraphTypeWrapper(classBuilder, "oidType", getOidType());
        addGraphTypeWrapper(classBuilder, "vdataType", getVdataType());

        String packageName = packageElement.getQualifiedName().toString();
        writeTypeSpec(packageName, classBuilder.build());

        generateGrapeWrapper();
        graphTypeWrapperGenerated = true;
    }

    private void writeTypeSpec(String packageName, TypeSpec typeSpec) {
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec).build();
        try {
            Filer filter = processingEnv.getFiler();
            javaFile.writeTo(filter);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot write Java file "
                            + javaFile.typeSpec.name
                            + ". Please clean the build first.",
                    e);
        }
    }

    private void addGraphTypeWrapper(TypeSpec.Builder classBuilder, String name, TypeMirror type) {
        MethodSpec.Builder methodBuilder =
                MethodSpec.methodBuilder(name).addModifiers(Modifier.ABSTRACT, Modifier.PUBLIC);
        methodBuilder.addAnnotation(FFIGetter.class);
        if (!isBoxedPrimitive(type)) {
            methodBuilder.addAnnotation(CXXReference.class);
        }
        methodBuilder.returns(TypeName.get(makeGraphWrapper(type)));
        classBuilder.addMethod(methodBuilder.build());
    }

    private TypeMirror makeGraphWrapper(TypeMirror type) {
        TypeElement vectorElement =
                processingEnv.getElementUtils().getTypeElement(FFIVector.class.getName());
        TypeMirror typeVector = processingEnv.getTypeUtils().getDeclaredType(vectorElement, type);
        TypeMirror typeVectorVector =
                processingEnv.getTypeUtils().getDeclaredType(vectorElement, typeVector);
        return typeVectorVector;
    }

    private boolean isBoxedPrimitive(TypeMirror typeMirror) {
        if (isSameType(typeMirror, Byte.class)) {
            return true;
        }
        if (isSameType(typeMirror, Boolean.class)) {
            return true;
        }
        if (isSameType(typeMirror, Short.class)) {
            return true;
        }
        if (isSameType(typeMirror, Character.class)) {
            return true;
        }
        if (isSameType(typeMirror, Integer.class)) {
            return true;
        }
        if (isSameType(typeMirror, Long.class)) {
            return true;
        }
        if (isSameType(typeMirror, Float.class)) {
            return true;
        }
        if (isSameType(typeMirror, Double.class)) {
            return true;
        }
        return false;
    }

    private void output() {
        if (!checkPrecondition()) {
            return;
        }
        outputOperators();
        outputGraphType();
    }

    private boolean checkPrecondition() {
        if (graphType == null) {
            return false;
        }
        if (!checkTypes(getVidType(), getOidType(), getVdataType(), getEdataType())) {
            return false;
        }
        return true;
    }

    private boolean checkTypes(TypeMirror... typeMirrors) {
        for (TypeMirror typeMirror : typeMirrors) {
            if (!checkType(typeMirror)) {
                return false;
            }
        }
        return true;
    }

    private boolean isFFIMirror(TypeMirror typeMirror) {
        if (typeMirror instanceof DeclaredType) {
            if (!isAssignable(typeMirror, FFIType.class)) {
                return false;
            }
            DeclaredType declaredType = (DeclaredType) typeMirror;
            FFIMirror ffiMirror = declaredType.asElement().getAnnotation(FFIMirror.class);
            return ffiMirror != null;
        }
        return false;
    }

    private boolean checkType(TypeMirror typeMirror) {
        if (isFFIMirror(typeMirror)) {
            DeclaredType declaredType = (DeclaredType) typeMirror;
            String foreignType = getForeignTypeNameByFFITypeAlias(declaredType);
            FFIMirrorDefinition mirrorDefinition =
                    foreignTypeNameToFFIMirrorDefinitionMap.get(foreignType);
            if (mirrorDefinition == null) {
                // this may happen if FFIMirror is not processed by ffi-annotation-processor yet.
                return false;
            }
            return true;
        }
        return true;
    }

    private String getAnnotationMember(AnnotationMirror annotationMirror, String name) {
        Map<? extends ExecutableElement, ? extends AnnotationValue> values =
                annotationMirror.getElementValues();
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
                values.entrySet()) {
            ExecutableElement executableElement = entry.getKey();
            if (executableElement.getSimpleName().toString().equals(name)) {
                AnnotationValue value = entry.getValue();
                return value.toString();
            }
        }
        throw new IllegalStateException("Cannot find key " + name + " in " + annotationMirror);
    }

    private String getGraphTypeMember(String name) {
        String literal = getAnnotationMember(graphType, name);
        if (!literal.endsWith(".class")) {
            throw new IllegalStateException("Must be a class literal: " + literal);
        }
        return literal.substring(0, literal.length() - ".class".length());
    }

    private String getGraphTypeMemberForeign(String name) {
        String tmp = getAnnotationMember(graphType, name);
        return tmp.substring(1, tmp.length() - 1);
    }

    private TypeMirror getTypeMirror(String name) {
        TypeElement typeElement = processingEnv.getElementUtils().getTypeElement(name);
        if (typeElement == null) {
            throw new IllegalStateException("Cannot get TypeElement for " + name);
        }
        return typeElement.asType();
    }

    private DeclaredType getVidType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("vidType"));
    }

    private DeclaredType getOidType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("oidType"));
    }

    private DeclaredType getVdataType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("vdataType"));
    }

    private DeclaredType getEdataType() {
        return (DeclaredType) getTypeMirror(getGraphTypeMember("edataType"));
    }

    private Set<String> getGraphTypeHeaders() {
        Set<String> headers = new HashSet<>();
        Arrays.asList(getOidType(), getVidType(), getVdataType(), getEdataType())
                .forEach(
                        cls -> {
                            String h = getForeignTypeHeader(cls);
                            if (h != null) {
                                headers.add(h);
                            }
                        });
        return headers;
    }

    private void outputGraphType() {
        if (this.graphType == null) {
            return;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        String header = "user_type_info.h";
        String guard = AnnotationProcessorUtils.toHeaderGuard(header);
        writer.append("#ifndef ").append(guard).append("\n");
        writer.append("#define ").append(guard).append("\n");
        writer.append("#include <cstdlib>\n");
        Set<String> headers = getGraphTypeHeaders();

        headers.stream()
                .sorted()
                .forEach(
                        h -> {
                            writer.append("#include ").append(h).append("\n");
                        });

        writer.append("\n");
        writer.append("namespace grape {\n");
        writer.format("using OID_T = %s;\n", getForeignTypeName(getOidType()));
        writer.format("using VID_T = %s;\n", getVidForeignTypeName(getVidType()));
        writer.format("using VDATA_T = %s;\n", getForeignTypeName(getVdataType()));
        writer.format("using EDATA_T = %s;\n", getForeignTypeName(getEdataType()));
        writer.append("\n\n");
        writer.format(
                "char const* OID_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getOidType()));
        writer.format(
                "char const* VID_T_str = \"std::vector<std::vector<%s>>\";\n",
                getVidForeignTypeName(getVidType()));
        writer.format(
                "char const* VDATA_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getVdataType()));
        writer.format(
                "char const* EDATA_T_str = \"std::vector<std::vector<%s>>\";\n",
                getForeignTypeName(getEdataType()));
        writer.append("} // end of namespace grape\n");
        writer.append("\n\n");
        writer.append("#endif // ").append(guard).append("\n");
        writer.flush();
        writeFile(header, outputStream.toString());
    }

    private String getVidForeignTypeName(TypeMirror vidType) {
        if (isSameType(vidType, Integer.class)) {
            return "uint32_t";
        }
        if (isSameType(vidType, Long.class)) {
            return "uint64_t";
        }
        throw new IllegalStateException("Unsupported vid type: " + vidType);
    }

    private void writeFile(String fileName, String fileContent) {
        try {
            FileObject fileObject =
                    processingEnv
                            .getFiler()
                            .createResource(StandardLocation.SOURCE_OUTPUT, "", fileName);
            try (Writer headerWriter = fileObject.openWriter()) {
                headerWriter.write(fileContent);
                headerWriter.flush();
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Cannot write " + fileName + " due to " + e.getMessage(), e);
        }
    }

    private boolean isSameType(TypeMirror typeMirror, Class<?> clazz) {
        TypeMirror typeMirror2 = getTypeMirror(clazz.getName());
        return processingEnv.getTypeUtils().isSameType(typeMirror, typeMirror2);
    }

    private boolean isAssignable(TypeMirror typeMirror, Class<?> clazz) {
        TypeMirror typeMirror2 = getTypeMirror(clazz.getName());
        return processingEnv.getTypeUtils().isAssignable(typeMirror, typeMirror2);
    }

    private String getTypeName(DeclaredType declaredType) {
        return ((TypeElement) declaredType.asElement()).getQualifiedName().toString();
    }

    private String getFFIMirrorForeignTypeName(FFIMirrorDefinition mirrorDefinition) {
        String namespace = mirrorDefinition.namespace();
        String name = mirrorDefinition.name();
        if (namespace.isEmpty()) {
            return name;
        }
        return namespace + "::" + name;
    }

    private String getForeignTypeName(TypeMirror typeMirror) {
        return getForeignTypeNameOrHeader(typeMirror, true);
    }

    private String getForeignTypeHeader(TypeMirror typeMirror) {
        return getForeignTypeNameOrHeader(typeMirror, false);
    }

    private String getForeignTypeNameOrHeader(TypeMirror typeMirror, boolean name) {
        if (typeMirror instanceof PrimitiveType) {
            throw new IllegalStateException("Please use boxed type instead.");
        }
        if (typeMirror instanceof ArrayType) {
            throw new IllegalStateException("No array is supported: " + typeMirror);
        }

        if (isSameType(typeMirror, Byte.class)) {
            return name ? "jbyte" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Boolean.class)) {
            return name ? "jboolean" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Short.class)) {
            return name ? "jshort" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Character.class)) {
            return name ? "jchar" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Integer.class)) {
            return name ? "jint" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Long.class)) {
            return name ? "jlong" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Float.class)) {
            return name ? "jfloat" : JNI_HEADER;
        }
        if (isSameType(typeMirror, Double.class)) {
            return name ? "jdouble" : JNI_HEADER;
        }
        if (isSameType(typeMirror, FFIByteString.class)) {
            return name ? "std::string" : "<string>";
        }
        if (typeMirror instanceof DeclaredType) {
            DeclaredType declaredType = (DeclaredType) typeMirror;
            if (!declaredType.getTypeArguments().isEmpty()) {
                throw new IllegalStateException(
                        "No parameterized type is supported: " + typeMirror);
            }
            if (!isAssignable(typeMirror, FFIType.class)) {
                throw new IllegalStateException("Must be an FFIType: " + typeMirror);
            }
            FFIMirror ffiMirror = declaredType.asElement().getAnnotation(FFIMirror.class);
            if (ffiMirror == null) {
                throw new IllegalStateException("Must be an FFIMirror: " + typeMirror);
            }
            String foreignTypeName = getForeignTypeNameByFFITypeAlias(declaredType);

            FFIMirrorDefinition mirrorDefinition =
                    foreignTypeNameToFFIMirrorDefinitionMap.get(foreignTypeName);
            if (mirrorDefinition == null) {
                throw new IllegalStateException(
                        "Cannot find FFIMirror implementation for "
                                + typeMirror
                                + "/"
                                + declaredType.asElement()
                                + " via foreign type "
                                + foreignTypeName);
            }
            if (!getFFIMirrorForeignTypeName(mirrorDefinition).equals(foreignTypeName)) {
                throw new IllegalStateException(
                        "Oops, expect " + foreignTypeName + ", got " + mirrorDefinition.name());
            }
            return name
                    ? getFFIMirrorForeignTypeName(mirrorDefinition)
                    : "\"" + mirrorDefinition.header() + "\"";
        }
        throw new IllegalStateException("Unsupported type: " + typeMirror);
    }

    private String getForeignTypeNameByFFITypeAlias(DeclaredType declaredType) {
        TypeElement typeElement = (TypeElement) declaredType.asElement();
        return getForeignTypeNameByFFITypeAlias(typeElement);
    }

    private String getForeignTypeNameByFFITypeAlias(TypeElement typeElement) {
        FFITypeAlias typeAlias = typeElement.getAnnotation(FFITypeAlias.class);
        FFINameSpace nameSpace = typeElement.getAnnotation(FFINameSpace.class);
        if (typeAlias == null || typeAlias.value().isEmpty()) {
            throw new IllegalStateException("No valid FFITypeAlias in " + typeElement);
        }
        if (nameSpace != null && !nameSpace.value().isEmpty()) {
            return nameSpace.value() + "::" + typeAlias.value();
        }
        return typeAlias.value();
    }

    private List<FFIMirrorFieldDefinition> sortedFields(FFIMirrorDefinition m) {
        List<FFIMirrorFieldDefinition> fields = new ArrayList<>(Arrays.asList(m.fields()));
        Collections.sort(fields, Comparator.comparing(FFIMirrorFieldDefinition::name));
        return fields;
    }

    private void outputOperators() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(outputStream);
        String grapeHeader = "grape-gen.h";
        String guard = AnnotationProcessorUtils.toHeaderGuard(grapeHeader);
        writer.append("#ifndef ").append(guard).append("\n");
        writer.append("#define ").append(guard).append("\n");
        writer.append("#include \"grape/serialization/in_archive.h\"\n");
        writer.append("#include \"grape/serialization/out_archive.h\"\n");

        List<FFIMirrorDefinition> mirrorDefinitionList =
                new ArrayList<>(foreignTypeNameToFFIMirrorDefinitionMap.values());
        Collections.sort(mirrorDefinitionList, Comparator.comparing(FFIMirrorDefinition::name));
        mirrorDefinitionList.forEach(m -> writer.format("#include \"%s\"\n", m.header()));
        mirrorDefinitionList.forEach(
                m -> {
                    String fullName = getFFIMirrorForeignTypeName(m);
                    String javaType = foreignTypeNameToJavaTypeName.get(fullName);
                    if (javaType == null) {
                        throw new IllegalStateException("Cannot get Java type for " + fullName);
                    }
                    if (javaType.endsWith(GraphTypeWrapperSuffix)) {
                        return;
                    }
                    String namespace = m.namespace();
                    if (!namespace.isEmpty()) {
                        writer.format("namespace %s {\n", m.namespace());
                    }
                    // outarchive
                    writer.format(
                            "inline grape::OutArchive& operator>>(grape::OutArchive& out_archive, %s& data) {\n",
                            m.name());
                    List<FFIMirrorFieldDefinition> fields = sortedFields(m);
                    fields.forEach(
                            f -> {
                                writer.format("\tout_archive >> data.%s;\n", f.name());
                            });
                    writer.format("\treturn out_archive;\n}\n");
                    // inarchive
                    writer.format(
                            "inline grape::InArchive& operator<<(grape::InArchive& in_archive, const %s& data) {\n",
                            m.name());
                    fields.forEach(
                            f -> {
                                writer.format("\tin_archive << data.%s;\n", f.name());
                            });
                    writer.format("\treturn in_archive;\n}\n");
                    // equal operator
                    writer.format(
                            "inline bool operator==(const %s& a, const %s& b) {\n",
                            m.name(), m.name());
                    fields.forEach(
                            f -> {
                                writer.format(
                                        "\tif (a.%s != b.%s) return false;\n", f.name(), f.name());
                            });
                    writer.format("\treturn true;\n}\n");

                    if (!namespace.isEmpty()) {
                        writer.format("} // end namespace %s\n", m.namespace());
                    }
                });

        {
            Map<String, String> all = collectFFIMirrorTypeMapping(processingEnv);
            all.forEach(
                    (foreignType, javaTypeName) -> {
                        if (javaTypeName.endsWith(GraphTypeWrapperSuffix)) {
                            return;
                        }
                        TypeMirror javaType =
                                AnnotationProcessorUtils.typeNameToDeclaredType(
                                        processingEnv, javaTypeName, (TypeElement) null);
                        if (javaType instanceof DeclaredType) {
                            DeclaredType declaredType = (DeclaredType) javaType;
                            if (!declaredType.getTypeArguments().isEmpty()) {
                                throw new IllegalStateException(
                                        "No parameterized type is allowed, got " + javaType);
                            }
                            writer.append("namespace std {\n");
                            writer.format("template<> struct hash<%s> {\n", foreignType);
                            writer.format(
                                    "\tstd::size_t operator()(%s const& s) const noexcept {\n",
                                    foreignType);
                            writer.append("\t\tstd::size_t h = 17;\n");
                            List<FFIMirrorFieldDefinition> fields =
                                    sortedFields(
                                            foreignTypeNameToFFIMirrorDefinitionMap.get(
                                                    foreignType));
                            for (FFIMirrorFieldDefinition field : fields) {
                                // llvm has no default std::hash impl for std::vector.
                                if (field.foreignType().indexOf("std::vector") == -1) {
                                    writer.format(
                                            "\t\th = h * 31 + std::hash<%s>()(s.%s);\n",
                                            field.foreignType(), field.name());
                                }
                            }
                            writer.append("\t\treturn h;\n");
                            writer.append("\t}\n");
                            writer.format("}; // end of std::hash<%s>\n", foreignType);
                            writer.append("} // end namespace std\n");
                        }
                    });
        }

        writer.append("#endif // ").append(guard).append("\n");
        writer.flush();
        writeFile(grapeHeader, outputStream.toString());
    }

    private void checkedRegisterType(
            ProcessingEnvironment processingEnv,
            String foreignType,
            String javaType,
            Map<String, String> mapping) {
        String check = mapping.put(foreignType, javaType);
        if (check != null && !check.equals(javaType)) {
            throw new IllegalStateException(
                    "Inconsistent type registered on "
                            + foreignType
                            + ", expected "
                            + check
                            + ", got "
                            + javaType);
        }
    }

    private Map<String, String> collectFFIMirrorTypeMapping(ProcessingEnvironment processingEnv) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, FFIMirrorDefinition> entry :
                javaTypeNameToFFIMirrorDefinitionMap.entrySet()) {
            {
                String javaTypeName = entry.getKey();
                String foreignType = getFFIMirrorForeignTypeName(entry.getValue());
                checkedRegisterType(processingEnv, foreignType, javaTypeName, map);
            }
            // for (FFIMirrorFieldDefinition mirrorFieldDefinition : entry.getValue().fields())
            // {
            // String javaTypeName = mirrorFieldDefinition.javaType();
            // String foreignType = mirrorFieldDefinition.foreignType();
            // TypeMirror javaType =
            // AnnotationProcessorUtils.typeNameToDeclaredType(processingEnv, javaTypeName,
            // (TypeElement) null); checkedRegisterType(processingEnv, foreignType,
            // javaType, map);
            // }
        }
        return map;
    }
}

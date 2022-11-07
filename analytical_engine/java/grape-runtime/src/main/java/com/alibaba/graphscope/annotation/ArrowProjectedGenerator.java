package com.alibaba.graphscope.annotation;

import static com.alibaba.graphscope.annotation.Utils.addCXXTemplate;
import static com.alibaba.graphscope.annotation.Utils.getMessageTypes;
import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.JavaClassName.STRING_VIEW;

import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.graphscope.context.ffi.FFIVertexDataContext;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.VertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.squareup.javapoet.AnnotationSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Class which add typespec to ffiGenBatch for arrowProjectedFragment. In annotation invoker, we
 * alread generated some classes, try to avoid regeneration.
 * TODO(zhanglei): generate according to message strategy, if provided.
 */
public class ArrowProjectedGenerator {

    private static Logger logger = LoggerFactory.getLogger(ArrowProjectedGenerator.class.getName());
    private static String LONG_MSG_FULL_NAME = "com.alibaba.graphscope.parallel.message.LongMsg";
    private static String DOUBLE_MSG_FULL_NAME =
            "com.alibaba.graphscope.parallel.message.DoubleMsg";
    private static String EMPTY_TYPE = "com.alibaba.graphscope.ds.EmptyType";
    private static String UNUSED_CLASS_NAME = "com.alibaba.graphscope.runtime.UnusedImpl";
    private AnnotationSpec.Builder ffiGenBatchBuilder;
    private String cppOid, cppVid, cppVdata, cppEdata;
    private String javaOid, javaVid, javaVdata, javaEdata;
    private String simpleJavaOid, simpleJavaVid, simpleJavaVdata, simpleJavaEdata;
    private String vertexDataType, javaVertexDataType;
    private String cppFragName, javaFragName;
    private String[][] messageTypePairs;

    private HashSet<String> KNOWN_VDATA =
            new HashSet<>(
                    Arrays.asList(
                            "Integer",
                            "java.lang.Integer",
                            "Long",
                            "java.lang.Long",
                            "Double",
                            "java.lang.Double",
                            STRING_VIEW));

    private HashSet<String> KNOWN_EDATA =
            new HashSet<>(
                    Arrays.asList(
                            "Integer",
                            "java.lang.Integer",
                            "Long",
                            "java.lang.Long",
                            "Double",
                            "java.lang.Double",
                            STRING_VIEW));

    private HashSet<String> KNOWN_VERTEX_DATAT =
            new HashSet<>(
                    Arrays.asList(
                            "Integer",
                            "java.lang.Integer",
                            "Long",
                            "java.lang.Long",
                            "Double",
                            "java.lang.Double"));

    public ArrowProjectedGenerator(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String cppOid,
            String cppVid,
            String cppVdata,
            String cppEdata,
            String javaOid,
            String javaVid,
            String javaVdata,
            String javaEdata,
            String vertexDataType,
            String javaVertexDataType) {
        this.ffiGenBatchBuilder = ffiGenBatchBuilder;
        this.cppOid = cppOid;
        this.cppVid = cppVid;
        this.cppVdata = cppVdata;
        this.cppEdata = cppEdata;
        this.javaOid = javaOid;
        this.javaVid = javaVid;
        this.javaVdata = javaVdata;
        this.javaEdata = javaEdata;
        this.vertexDataType = vertexDataType;
        this.javaVertexDataType = javaVertexDataType;
        this.simpleJavaOid = typeNameToSimple(javaOid);
        this.simpleJavaVid = typeNameToSimple(javaVid);
        this.simpleJavaVdata = typeNameToSimple(javaVdata);
        this.simpleJavaEdata = typeNameToSimple(javaEdata);
        cppFragName =
                String.format(
                        "%s<%s,%s,%s,%s>",
                        CPP_ARROW_PROJECTED_FRAGMENT, cppOid, cppVid, cppVdata, cppEdata);
        javaFragName =
                String.format(
                        "%s<%s,%s,%s,%s>",
                        ArrowProjectedFragment.class.getName(),
                        javaOid,
                        javaVid,
                        javaVdata,
                        javaEdata);
        logger.info("cpp frag name: {}, java frag name {}", cppFragName, javaFragName);

        List<String> messageTypes = getMessageTypes();
        if (messageTypes.isEmpty()) {
            logger.warn("No message types provided");
        } else {
            logger.info("received msg types {}", String.join(",", messageTypes));
            messageTypePairs = new String[messageTypes.size()][];
            for (int i = 0; i < messageTypes.size(); ++i) {
                String messageType = messageTypes.get(i);
                String[] types = messageType.split("=");
                if (types.length != 2) {
                    throw new IllegalStateException("format error, expect a=b: " + messageType);
                }
                messageTypePairs[i] = types;
            }
        }
    }

    public void generate() {
        // vid
        if (javaVid.equals("Long") || javaVid.equals("java.lang.Long")) {
            logger.info("Skip generating for vid {}", javaVdata);
        } else {
            addTemplate(
                    PropertyNbrUnit.class.getName(),
                    Collections.singletonList(cppVid),
                    Collections.singletonList(javaVid));
            addTemplate(
                    VertexRange.class.getName(),
                    Collections.singletonList(cppVid),
                    Collections.singletonList(javaVid));
        }

        // vd and ed
        List<String> cpps = new ArrayList<>(2);
        List<String> javas = new ArrayList<>(2);
        boolean eDataContained = false;
        if (KNOWN_VDATA.contains(javaVdata)) {
            logger.info("Skip generating primitive typed array for type {}", javaVdata);
        } else {
            cpps.add(cppVdata);
            javas.add(javaVdata);
        }
        if (KNOWN_EDATA.contains(javaEdata)) {
            logger.info("Skip generating primitive typed array for type {}", javaEdata);
            eDataContained = true;
        } else {
            cpps.add(cppEdata);
            javas.add(javaEdata);
        }
        if (cpps.size() > 0) {
            addTemplate(PrimitiveTypedArray.class.getName(), cpps, javas);
            addArrowProjectedFragment();
            addVertexDataContext(ffiGenBatchBuilder, cppFragName, javaFragName);
        }
        if (eDataContained) {
            logger.info("Skip generating ProjectedNbr for edata {}", javaEdata);
        } else {
            addTemplate(ProjectedNbr.class.getName(), cppVid, cppEdata, javaVid, javaEdata);
            addTemplate(ProjectedAdjList.class.getName(), cppVid, cppEdata, javaVid, javaEdata);
        }

        if (KNOWN_VERTEX_DATAT.contains(javaVertexDataType)) {
            logger.info("Skip generating vertex data array for {}", javaVertexDataType);
        } else {
            addVertexArray();
        }

        // always generate for messages.
        addParallelMessageManager();
        addMessageInBuffer();
    }

    private void addVertexArray() {
        AnnotationSpec.Builder ffiGen1 = AnnotationSpec.builder(FFIGen.class);
        ffiGen1.addMember("type", "$S", VertexArray.class.getName());
        addCXXTemplate(ffiGen1, "uint64_t", vertexDataType, "Long", javaVertexDataType);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGen1.build());

        addTemplate(
                GSVertexArray.class.getName(),
                Collections.singletonList(vertexDataType),
                Collections.singletonList(javaVertexDataType));
    }

    private void addVertexDataContext(
            AnnotationSpec.Builder ffiGenBatchBuilder,
            String foreignFragName,
            String javaFragName) {
        AnnotationSpec.Builder ffiGenVertexDataContext = AnnotationSpec.builder(FFIGen.class);
        ffiGenVertexDataContext.addMember("type", "$S", FFIVertexDataContext.class.getName());
        ffiGenVertexDataContext.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", foreignFragName)
                        .addMember("cxx", "$S", vertexDataType)
                        .addMember("java", "$S", javaFragName)
                        .addMember("java", "$S", javaVertexDataType)
                        .build());
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenVertexDataContext.build());
    }

    private void addProjectedAdjList() {
        addTemplate(ProjectedAdjList.class.getName(), cppVid, cppEdata, javaVid, javaEdata);
    }

    private void addArrowProjectedFragment() {
        addTemplate(
                ArrowProjectedFragment.class.getName(),
                cppOid,
                cppVid,
                cppVdata,
                cppEdata,
                javaOid,
                javaVid,
                javaVdata,
                javaEdata);
    }

    private void addParallelMessageManager() {
        AnnotationSpec.Builder ffiGenBuilderParallel = AnnotationSpec.builder(FFIGen.class);
        ffiGenBuilderParallel.addMember("type", "$S", ParallelMessageManager.class.getName());
        parallelMessageManagerAddMessages(ffiGenBuilderParallel);
        ffiGenBatchBuilder.addMember("value", "$L", ffiGenBuilderParallel.build());
    }

    private void addMessageInBuffer() {
        AnnotationSpec.Builder curBuilder = AnnotationSpec.builder(FFIGen.class);
        curBuilder.addMember("type", "$S", MessageInBuffer.class.getName());
        {
            // add getPureMessage
            AnnotationSpec.Builder methodBuilder =
                    AnnotationSpec.builder(FFIFunGen.class)
                            .addMember("name", "$S", "getPureMessage")
                            .addMember("returnType", "$S", "boolean")
                            .addMember("parameterTypes", "$S", "MSG_T");
            AnnotationSpec.Builder templateBuilder =
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", "std::vector<char>")
                            .addMember("java", "$S", "com.alibaba.graphscope.stdcxx.FFIByteVector");
            methodBuilder.addMember("templates", "$L", templateBuilder.build());
            curBuilder.addMember("functionTemplates", "$L", methodBuilder.build());
        }
        addFuncGenMethod(
                curBuilder,
                "getMessageArrowProjected",
                "boolean",
                new String[] {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "VDATA_T"},
                messageTypePairs);
        ffiGenBatchBuilder.addMember("value", "$L", curBuilder.build());
    }

    private void addTemplate(String typeName, String... templates) {
        if (templates.length % 2 != 0) {
            throw new IllegalStateException(
                    "Can not have odd number of template types " + String.join(",", templates));
        }
        int num = templates.length;
        AnnotationSpec.Builder curBuilder = AnnotationSpec.builder(FFIGen.class);
        curBuilder.addMember("type", "$S", typeName);
        // build cxx template.
        AnnotationSpec.Builder cxxTemplateBuilder = AnnotationSpec.builder(CXXTemplate.class);
        for (int i = 0; i < num / 2; ++i) {
            cxxTemplateBuilder.addMember("cxx", "$S", templates[i]);
        }
        for (int i = num / 2; i < num; ++i) {
            cxxTemplateBuilder.addMember("java", "$S", templates[i]);
        }
        curBuilder.addMember("templates", "$L", cxxTemplateBuilder.build());
        ffiGenBatchBuilder.addMember("value", "$L", curBuilder.build());
    }

    /**
     * add template with multiple cxx template
     */
    private void addTemplate(String typeName, List<String> cxxs, List<String> javas) {
        if (cxxs.size() != javas.size()) {
            throw new IllegalStateException("Java cxx template length not equal");
        }
        int num = cxxs.size();
        AnnotationSpec.Builder curBuilder = AnnotationSpec.builder(FFIGen.class);
        curBuilder.addMember("type", "$S", typeName);
        // build cxx template.
        for (int i = 0; i < num; ++i) {
            AnnotationSpec.Builder cxxTemplateBuilder = AnnotationSpec.builder(CXXTemplate.class);
            cxxTemplateBuilder.addMember("cxx", "$S", cxxs.get(i));
            cxxTemplateBuilder.addMember("java", "$S", javas.get(i));
            curBuilder.addMember("templates", "$L", cxxTemplateBuilder.build());
        }

        ffiGenBatchBuilder.addMember("value", "$L", curBuilder.build());
    }

    private void parallelMessageManagerAddMessages(AnnotationSpec.Builder parallelMessageBuilder) {

        addFuncGenMethod(
                parallelMessageBuilder,
                "sendMsgThroughIEdgesArrowProjected",
                "void",
                new String[] {
                    "FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"
                },
                messageTypePairs);
        addFuncGenMethod(
                parallelMessageBuilder,
                "sendMsgThroughOEdgesArrowProjected",
                "void",
                new String[] {
                    "FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"
                },
                messageTypePairs);
        addFuncGenMethod(
                parallelMessageBuilder,
                "sendMsgThroughEdgesArrowProjected",
                "void",
                new String[] {
                    "FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"
                },
                messageTypePairs);
        addFuncGenMethod(
                parallelMessageBuilder,
                "syncStateOnOuterVertexArrowProjected",
                "void",
                new String[] {
                    "FRAG_T", "com.alibaba.graphscope.ds.Vertex", "MSG_T", "int", "UNUSED_T"
                },
                messageTypePairs);
        addFuncGenMethodNoMsg(
                parallelMessageBuilder,
                "syncStateOnOuterVertexArrowProjectedNoMsg",
                "void",
                new String[] {"FRAG_T", "com.alibaba.graphscope.ds.Vertex", "int", "UNUSED_T"});
    }

    public void addFuncGenMethod(
            AnnotationSpec.Builder parallelMessageBuilder,
            String methodName,
            String returnType,
            String[] parameterTypes,
            String[][] messageTypePairs) {
        AnnotationSpec.Builder methodBuilder =
                AnnotationSpec.builder(FFIFunGen.class)
                        .addMember("name", "$S", methodName)
                        .addMember("returnType", "$S", returnType);
        for (String type : parameterTypes) {
            methodBuilder.addMember("parameterTypes", "$S", type);
        }
        if (messageTypePairs != null) {
            for (String[] types : messageTypePairs) {
                AnnotationSpec.Builder templateBuilder =
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember("cxx", "$S", cppOid)
                                .addMember("cxx", "$S", cppVid)
                                .addMember("cxx", "$S", cppVdata)
                                .addMember("cxx", "$S", cppEdata)
                                .addMember("cxx", "$S", cppFragName)
                                .addMember("cxx", "$S", types[0])
                                .addMember("cxx", "$S", "whateverThisIs")
                                .addMember("java", "$S", javaOid)
                                .addMember("java", "$S", javaVid)
                                .addMember("java", "$S", javaVdata)
                                .addMember("java", "$S", javaEdata)
                                .addMember("java", "$S", javaFragName)
                                .addMember("java", "$S", types[1])
                                .addMember("java", "$S", getUnusedTypeName(types[1]));
                methodBuilder.addMember("templates", "$L", templateBuilder.build());
            }
        } else {
            logger.info("skip generation for method {} since no msg type available ", methodName);
        }
        parallelMessageBuilder.addMember("functionTemplates", "$L", methodBuilder.build());
    }

    public void addFuncGenMethodNoMsg(
            AnnotationSpec.Builder parallelMessageBuilder,
            String methodName,
            String returnType,
            String[] parameterTypes) {
        AnnotationSpec.Builder methodBuilder =
                AnnotationSpec.builder(FFIFunGen.class)
                        .addMember("name", "$S", methodName)
                        .addMember("returnType", "$S", returnType);
        for (String type : parameterTypes) {
            methodBuilder.addMember("parameterTypes", "$S", type);
        }
        AnnotationSpec.Builder templateBuilder =
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", cppOid)
                        .addMember("cxx", "$S", cppVid)
                        .addMember("cxx", "$S", cppVdata)
                        .addMember("cxx", "$S", cppEdata)
                        .addMember("cxx", "$S", cppFragName)
                        .addMember("cxx", "$S", "whateverThisIs")
                        .addMember("java", "$S", javaOid)
                        .addMember("java", "$S", javaVid)
                        .addMember("java", "$S", javaVdata)
                        .addMember("java", "$S", javaEdata)
                        .addMember("java", "$S", javaFragName)
                        // FIXME: make unused accepts two.
                        .addMember("java", "$S", getUnusedTypeName()); // anything should be ok.
        methodBuilder.addMember("templates", "$L", templateBuilder.build());
        parallelMessageBuilder.addMember("functionTemplates", "$L", methodBuilder.build());
    }

    public String getUnusedTypeName(String msgType) {
        return UNUSED_CLASS_NAME
                + "."
                + simpleJavaVdata
                + simpleJavaEdata
                + typeNameToSimple(msgType);
    }

    public String getUnusedTypeName() {
        return UNUSED_CLASS_NAME + "." + simpleJavaVdata + simpleJavaEdata;
    }

    private static String typeNameToSimple(String fullName) {
        if (fullName.equals("java.lang.Long") || fullName.equals(LONG_MSG_FULL_NAME)) {
            return "Long";
        } else if (fullName.equals("java.lang.Double") || fullName.equals(DOUBLE_MSG_FULL_NAME)) {
            return "Double";
        } else if (fullName.equals("java.lang.Integer")) {
            return "Integer";
        } else if (fullName.equals("java.lang.String")
                || fullName.equals("com.alibaba.fastffi.FFIByteString")
                || fullName.equals("com.alibaba.graphscope.ds.StringView")) {
            return "String";
        } else if (fullName.equals("Empty") || fullName.equals(EMPTY_TYPE)) {
            return "Empty";
        } else {
            throw new IllegalStateException("Unrecognized full name " + fullName);
        }
    }
}

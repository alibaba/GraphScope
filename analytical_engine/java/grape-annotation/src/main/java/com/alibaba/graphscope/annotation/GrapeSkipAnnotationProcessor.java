package com.alibaba.graphscope.annotation;

import com.alibaba.fastffi.annotation.AnnotationProcessorUtils;
import com.alibaba.graphscope.utils.Unused;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic.Kind;

@SupportedAnnotationTypes("com.alibaba.graphscope.annotation.GrapeSkip")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GrapeSkipAnnotationProcessor extends AbstractProcessor {

    public Messager messager;

    private final Set<String> LEGAL_TYPES =
            new HashSet<String>(Arrays.asList("Long", "Integer", "Double", "String", "Empty"));

    /**
     * {@inheritDoc}
     *
     * @param annotations
     * @param roundEnv
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        //        if (roundEnv.processingOver()) {
        //            output();
        //            return false;
        //        }
        messager = processingEnv.getMessager();
        for (TypeElement annotation : annotations) {
            for (Element e : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (e instanceof TypeElement) {
                    processAnnotation((TypeElement) e, annotation);
                }
            }
        }
        return false;
    }

    void processAnnotation(TypeElement typeElement, TypeElement annotation) {
        messager.printMessage(
                Kind.NOTE,
                "visiting element "
                        + typeElement.getSimpleName()
                        + ", annotation "
                        + annotation.getSimpleName(),
                typeElement);
        if (isSameType(annotation.asType(), GrapeSkip.class)) {
            GrapeSkip[] grapeSkips = typeElement.getAnnotationsByType(GrapeSkip.class);
            if (grapeSkips.length != 1) {
                throw new IllegalStateException("GrapeSkip shall only appear once");
            }
            checkAndGenerate(typeElement, grapeSkips[0]);
            return;
        }
    }

    public String checkAndGenerate(TypeElement typeElement, GrapeSkip grapeSkip) {
        String dstClassName = "UnusedImpl";
        messager.printMessage(
                Kind.NOTE, "Processing element " + typeElement.getSimpleName(), typeElement);
        PackageElement packageElement = AnnotationProcessorUtils.getPackageElement(typeElement);
        String packageName = packageElement.getQualifiedName().toString();
        messager.printMessage(Kind.NOTE, "package name" + packageName, typeElement);

        TypeSpec.Builder classBuilder =
                TypeSpec.classBuilder(dstClassName)
                        .addModifiers(Modifier.PUBLIC)
                        .addSuperinterface(Unused.class);
        // collect vd,ed,msg types
        String[] vdTypes = grapeSkip.vertexDataTypes();
        String[] edTypes = grapeSkip.edgeDataTypes();
        String[] msgTypes = grapeSkip.msgDataTypes();
        check(vdTypes);
        check(edTypes);
        check(msgTypes);

        // enumerate all possible classes;
        int numClasses = msgTypes.length * edTypes.length * vdTypes.length;
        String[] classNames = new String[numClasses];
        generateClassNames(numClasses, classNames, vdTypes, edTypes, msgTypes);
        messager.printMessage(Kind.NOTE, "generating " + numClasses + " skipClasses", typeElement);
        // vd+ed skip classes.
        String[] vedClassesNames = new String[vdTypes.length * edTypes.length];
        generateClassNames(vdTypes.length * edTypes.length, vedClassesNames, vdTypes, edTypes);

        // filling class first;
        fillInSkipClasses(classBuilder, classNames);
        fillInSkipClasses(classBuilder, vedClassesNames);
        messager.printMessage(Kind.NOTE, "Finish filling skip classes", typeElement);
        //
        // add factory impl
        addMethod(classBuilder, classNames, edTypes.length * msgTypes.length, msgTypes.length);
        addVEMethod(classBuilder, vedClassesNames, edTypes.length);
        messager.printMessage(Kind.NOTE, "Finish add factory class", typeElement);

        writeTypeSpec(packageName, classBuilder.build(), typeElement);
        return dstClassName;
    }

    public void generateClassNames(
            int numClasses,
            String[] classNames,
            String[] vdTypes,
            String[] edTypes,
            String[] msgTypes) {
        int ind = 0;
        for (String vdType : vdTypes) {
            for (String edType : edTypes) {
                for (String msgType : msgTypes) {
                    String cur = vdType + edType + msgType;
                    classNames[ind] = cur;
                    ind += 1;
                }
            }
        }
    }

    public void generateClassNames(
            int numClasses, String[] classNames, String[] vdTypes, String[] edTypes) {
        int ind = 0;
        for (String vdType : vdTypes) {
            for (String edType : edTypes) {
                String cur = vdType + edType;
                classNames[ind] = cur;
                ind += 1;
            }
        }
    }

    public void fillInSkipClasses(TypeSpec.Builder builder, String[] classNames) {
        for (String className : classNames) {
            TypeSpec.Builder innerBuilder =
                    TypeSpec.classBuilder(className)
                            .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
                            .addSuperinterface(Unused.class);
            builder.addType(innerBuilder.build());
        }
    }

    public void addMethod(
            TypeSpec.Builder builder, String[] className, int edMultiplyMsg, int msgNum) {
        builder.addField(Unused[].class, "skips", Modifier.STATIC, Modifier.PRIVATE);
        CodeBlock.Builder staticCodeBuilder = CodeBlock.builder();
        staticCodeBuilder.addStatement(
                "skips = new com.alibaba.graphscope.utils.Unused[$L]", className.length);
        for (int i = 0; i < className.length; ++i) {
            staticCodeBuilder.addStatement("skips[$L] = new $L()", i, className[i]);
        }
        builder.addStaticBlock(staticCodeBuilder.build());

        MethodSpec.Builder getUnused =
                MethodSpec.methodBuilder("getUnused")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addParameter(ParameterSpec.builder(Class.class, "vd").build())
                        .addParameter(ParameterSpec.builder(Class.class, "ed").build())
                        .addParameter(ParameterSpec.builder(Class.class, "msg").build())
                        .addCode(
                                CodeBlock.builder()
                                        .addStatement(
                                                "int a ="
                                                    + " com.alibaba.graphscope.utils.Unused.class2Int(vd)")
                                        .addStatement(
                                                "int b ="
                                                    + " com.alibaba.graphscope.utils.Unused.class2Int(ed)")
                                        .addStatement(
                                                "int c ="
                                                    + " com.alibaba.graphscope.utils.Unused.class2Int(msg)")
                                        .addStatement(
                                                "int ind = a * $L + b * $L + c",
                                                edMultiplyMsg,
                                                msgNum)
                                        .addStatement("return skips[ind]")
                                        .build())
                        .returns(Unused.class);
        builder.addMethod(getUnused.build());
    }

    /** method for getUnused with only vd + ed */
    public void addVEMethod(TypeSpec.Builder builder, String[] className, int edNum) {
        builder.addField(Unused[].class, "veSkips", Modifier.STATIC, Modifier.PRIVATE);
        CodeBlock.Builder staticCodeBuilder = CodeBlock.builder();
        staticCodeBuilder.addStatement(
                "veSkips = new com.alibaba.graphscope.utils.Unused[$L]", className.length);
        for (int i = 0; i < className.length; ++i) {
            staticCodeBuilder.addStatement("veSkips[$L] = new $L()", i, className[i]);
        }
        builder.addStaticBlock(staticCodeBuilder.build());

        MethodSpec.Builder getUnused =
                MethodSpec.methodBuilder("getUnused")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                        .addParameter(ParameterSpec.builder(Class.class, "vd").build())
                        .addParameter(ParameterSpec.builder(Class.class, "ed").build())
                        .addCode(
                                CodeBlock.builder()
                                        .addStatement(
                                                "int a ="
                                                    + " com.alibaba.graphscope.utils.Unused.class2Int(vd)")
                                        .addStatement(
                                                "int b ="
                                                    + " com.alibaba.graphscope.utils.Unused.class2Int(ed)")
                                        .addStatement("int ind = a * $L + b", edNum)
                                        .addStatement("return veSkips[ind]")
                                        .build())
                        .returns(Unused.class);
        builder.addMethod(getUnused.build());
    }

    private void check(String[] types) {
        if (types.length <= 0) {
            throw new IllegalStateException("Empty types!");
        }
        for (String type : types) {
            if (!LEGAL_TYPES.contains(type)) {
                throw new IllegalStateException("Unrecognized type " + type);
            }
        }
    }

    private void writeTypeSpec(String packageName, TypeSpec typeSpec, TypeElement typeElement) {
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

    private boolean isSameType(TypeMirror typeMirror, Class<?> clazz) {
        TypeMirror typeMirror2 = getTypeMirror(clazz.getName());
        return processingEnv.getTypeUtils().isSameType(typeMirror, typeMirror2);
    }

    private TypeMirror getTypeMirror(String name) {
        TypeElement typeElement = processingEnv.getElementUtils().getTypeElement(name);
        if (typeElement == null) {
            throw new IllegalStateException("Cannot get TypeElement for " + name);
        }
        return typeElement.asType();
    }
}

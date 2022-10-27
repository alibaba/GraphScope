#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArrayBuilder_1cxx_10xb067eed0__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowStringArrayBuilder);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArrayBuilder_1cxx_10xb067eed0_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* additionalCapacity0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowStringArrayBuilder*>(ptr)->Reserve(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArrayBuilder_1cxx_10xb067eed0_nativeReserveData(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* additionalBytes0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) arrow::Status(reinterpret_cast<gs::ArrowStringArrayBuilder*>(ptr)->ReserveData(arg0)));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArrayBuilder_1cxx_10xb067eed0_nativeUnsafeAppend(JNIEnv*, jclass, jlong ptr, jlong arg0 /* ptr0 */, jint arg1 /* length1 */) {
	reinterpret_cast<gs::ArrowStringArrayBuilder*>(ptr)->UnsafeAppend(*reinterpret_cast<const char*>(arg0), arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArrayBuilder_1cxx_10xb067eed0_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowStringArrayBuilder());
}

#ifdef __cplusplus
}
#endif

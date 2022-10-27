#include <string>
#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArray_1cxx_10x49d2fe4b__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowStringArray);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_arrow_array_ArrowStringArray_1cxx_10x49d2fe4b_nativeGetString(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* index0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<gs::ArrowStringArray*>(ptr)->GetString(arg0)));
}

#ifdef __cplusplus
}
#endif

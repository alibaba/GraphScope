#include <jni.h>
#include <new>
#include "grape/types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_EmptyType_1cxx_10xf41b6c9c__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::EmptyType);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_EmptyType_1cxx_10xf41b6c9c_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::EmptyType());
}

#ifdef __cplusplus
}
#endif

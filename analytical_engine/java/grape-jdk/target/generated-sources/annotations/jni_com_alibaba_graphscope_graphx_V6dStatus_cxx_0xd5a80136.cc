#include <jni.h>
#include <new>
#include "vineyard/common/util/status.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_V6dStatus_1cxx_10xd5a80136__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::Status);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_graphx_V6dStatus_1cxx_10xd5a80136_nativeOk(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<vineyard::Status*>(ptr)->ok()) ? JNI_TRUE : JNI_FALSE;
}

#ifdef __cplusplus
}
#endif

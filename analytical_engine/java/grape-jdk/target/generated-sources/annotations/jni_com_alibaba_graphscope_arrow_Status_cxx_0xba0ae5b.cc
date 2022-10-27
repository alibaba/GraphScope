#include <jni.h>
#include <new>
#include "arrow/status.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_arrow_Status_1cxx_10xba0ae5b__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(arrow::Status);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_arrow_Status_1cxx_10xba0ae5b_nativeOk(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<arrow::Status*>(ptr)->ok()) ? JNI_TRUE : JNI_FALSE;
}

#ifdef __cplusplus
}
#endif

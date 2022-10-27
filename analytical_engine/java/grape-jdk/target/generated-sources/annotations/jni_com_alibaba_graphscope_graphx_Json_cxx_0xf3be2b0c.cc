#include <string>
#include <jni.h>
#include <new>
#include "vineyard/common/util/json.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_Json_1cxx_10xf3be2b0c__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::json);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_Json_1cxx_10xf3be2b0c_nativeDump(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<vineyard::json*>(ptr)->dump()));
}

#ifdef __cplusplus
}
#endif

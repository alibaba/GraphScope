#include <jni.h>
#include <new>
#include "stdint.h"
#include <map>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdMap_1cxx_10x7fd2b116__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::map<uint64_t,vineyard::json>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdMap_1cxx_10x7fd2b116_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* key0 */) {
	return reinterpret_cast<jlong>(&((*reinterpret_cast<std::map<uint64_t,vineyard::json>*>(ptr))[arg0]));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdMap_1cxx_10x7fd2b116_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::map<uint64_t,vineyard::json>());
}

#ifdef __cplusplus
}
#endif

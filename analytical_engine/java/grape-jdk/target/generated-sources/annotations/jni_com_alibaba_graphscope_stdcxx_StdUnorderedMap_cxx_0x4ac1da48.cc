#include <jni.h>
#include <new>
#include "stdint.h"
#include <unordered_map>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::unordered_map<unsigned,uint64_t>);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::unordered_map<unsigned,uint64_t>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<std::unordered_map<unsigned,uint64_t>*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeGet(JNIEnv*, jclass, jlong ptr, jint arg0 /* key0 */) {
	return (jlong)((*reinterpret_cast<std::unordered_map<unsigned,uint64_t>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeSet(JNIEnv*, jclass, jlong ptr, jint arg0 /* key0 */, jlong arg1 /* value1 */) {
	(*reinterpret_cast<std::unordered_map<unsigned,uint64_t>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<std::unordered_map<unsigned,uint64_t>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdUnorderedMap_1cxx_10x4ac1da48_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::unordered_map<unsigned,uint64_t>());
}

#ifdef __cplusplus
}
#endif

#include <jni.h>
#include <new>
#include <vector>
#include <string>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<int32_t>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<int32_t>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<int32_t>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<int32_t>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<int32_t>*>(ptr);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */) {
	return (jint)((*reinterpret_cast<std::vector<int32_t>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativePush_1back(JNIEnv*, jclass, jlong ptr, jint arg0 /* e0 */) {
	reinterpret_cast<std::vector<int32_t>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<std::vector<int32_t>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<std::vector<int32_t>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */, jint arg1 /* value1 */) {
	(*reinterpret_cast<std::vector<int32_t>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<int32_t>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xe1443cf1_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<int32_t>());
}

#ifdef __cplusplus
}
#endif

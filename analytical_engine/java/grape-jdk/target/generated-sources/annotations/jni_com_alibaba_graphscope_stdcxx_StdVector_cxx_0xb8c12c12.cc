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
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<char>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<char>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<char>*>(ptr);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */) {
	return (jbyte)((*reinterpret_cast<std::vector<char>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativePush_1back(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* e0 */) {
	reinterpret_cast<std::vector<char>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<std::vector<char>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<std::vector<char>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */, jbyte arg1 /* value1 */) {
	(*reinterpret_cast<std::vector<char>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdVector_1cxx_10xb8c12c12_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<char>());
}

#ifdef __cplusplus
}
#endif

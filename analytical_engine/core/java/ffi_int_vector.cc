#include <jni.h>
#include <new>
#include <string>
#include <vector>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIIntVector__1elementSize_00024_00024_00024(
    JNIEnv*, jclass) {
  return (jint) sizeof(std::vector<int>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeCapacity(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<int>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeClear(
    JNIEnv*, jclass, jlong ptr) {
  reinterpret_cast<std::vector<int>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeData(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<int>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeDelete(
    JNIEnv*, jclass, jlong ptr) {
  delete reinterpret_cast<std::vector<int>*>(ptr);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeGet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  return (jint)((*reinterpret_cast<std::vector<int>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativePush_1back(
    JNIEnv*, jclass, jlong ptr, jint arg0 /* arg00 */) {
  reinterpret_cast<std::vector<int>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeReserve(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<int>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeResize(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<int>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeSet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jint arg1 /* arg11 */) {
  (*reinterpret_cast<std::vector<int>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeSize(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<int>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIIntVector_nativeCreateFactory0(JNIEnv*,
                                                                     jclass) {
  return reinterpret_cast<jlong>(new std::vector<int>());
}

#ifdef __cplusplus
}
#endif

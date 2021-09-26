#ifndef ANALYTICAL_ENGINE_CORE_JAVA_FFI_BYTE_VECTOR_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_FFI_BYTE_VECTOR_H_

#ifdef ENABLE_JAVA_SDK

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
Java_com_alibaba_graphscope_stdcxx_FFIByteVector__1elementSize_00024_00024_00024(
    JNIEnv*, jclass) {
  return (jint) sizeof(std::vector<char>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeCapacity(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeClear(
    JNIEnv*, jclass, jlong ptr) {
  reinterpret_cast<std::vector<char>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeData(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeDelete(
    JNIEnv*, jclass, jlong ptr) {
  delete reinterpret_cast<std::vector<char>*>(ptr);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeGet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  return (jbyte)((*reinterpret_cast<std::vector<char>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativePush_1back(
    JNIEnv*, jclass, jlong ptr, jbyte arg0 /* arg00 */) {
  reinterpret_cast<std::vector<char>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeReserve(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<char>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeResize(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<char>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeSet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */,
    jbyte arg1 /* arg11 */) {
  (*reinterpret_cast<std::vector<char>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeSize(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(reinterpret_cast<std::vector<char>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIByteVector_nativeCreateFactory0(JNIEnv*,
                                                                      jclass) {
  return reinterpret_cast<jlong>(new std::vector<char>());
}

#ifdef __cplusplus
}
#endif

#endif
#endif
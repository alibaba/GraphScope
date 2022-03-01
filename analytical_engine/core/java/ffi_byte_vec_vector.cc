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
Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector__1elementSize_00024_00024_00024(
    JNIEnv*, jclass) {
  return (jint) sizeof(std::vector<std::vector<char>>);
}

JNIEXPORT
jlong JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeCapacity(JNIEnv*,
                                                                   jclass,
                                                                   jlong ptr) {
  return (jlong)(
      reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeClear(
    JNIEnv*, jclass, jlong ptr) {
  reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeData(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(
      reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeDelete(
    JNIEnv*, jclass, jlong ptr) {
  delete reinterpret_cast<std::vector<std::vector<char>>*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeGet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  return reinterpret_cast<jlong>(
      &((*reinterpret_cast<std::vector<std::vector<char>>*>(ptr))[arg0]));
}

JNIEXPORT
void JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativePush_1back(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->push_back(
      *reinterpret_cast<std::vector<char>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeReserve(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeResize(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
  reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeSet(
    JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */,
    jlong arg1 /* arg11 */) {
  (*reinterpret_cast<std::vector<std::vector<char>>*>(ptr))[arg0] =
      *reinterpret_cast<std::vector<char>*>(arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeSize(
    JNIEnv*, jclass, jlong ptr) {
  return (jlong)(
      reinterpret_cast<std::vector<std::vector<char>>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL
Java_com_alibaba_graphscope_stdcxx_FFIByteVecVector_nativeCreateFactory0(
    JNIEnv*, jclass) {
  return reinterpret_cast<jlong>(new std::vector<std::vector<char>>());
}

#ifdef __cplusplus
}
#endif


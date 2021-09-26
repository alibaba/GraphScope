/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
// This file contains neccessary code enableing porting a
// std::vector<std::vector<char>> to a java byte vecvector, We don't generate
// these jni files since the generated Java FFIByteVector class has been
// modified for optimization.

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

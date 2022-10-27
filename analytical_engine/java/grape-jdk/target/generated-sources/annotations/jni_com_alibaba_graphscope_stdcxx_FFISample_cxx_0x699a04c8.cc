#include "grape-jni-ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

namespace sample {
// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(sample::FFIMirrorSample);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<sample::FFIMirrorSample*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeDoubleVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->doubleVectorField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeDoubleVectorVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->doubleVectorVectorField));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeIntField0(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->intField);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeIntField1(JNIEnv*, jclass, jlong ptr, jint arg0 /* value0 */) {
	reinterpret_cast<sample::FFIMirrorSample*>(ptr)->intField = arg0;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeIntVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->intVectorField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeIntVectorVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->intVectorVectorField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeLongVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->longVectorField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeLongVectorVectorField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->longVectorVectorField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeStringField(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->stringField));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeVectorBytes(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<sample::FFIMirrorSample*>(ptr)->vectorBytes));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new sample::FFIMirrorSample());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_FFISample_1cxx_10x699a04c8_nativeCreateStackFactory1(JNIEnv*, jclass, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) sample::FFIMirrorSample());
}

} // end namespace sample
#ifdef __cplusplus
}
#endif

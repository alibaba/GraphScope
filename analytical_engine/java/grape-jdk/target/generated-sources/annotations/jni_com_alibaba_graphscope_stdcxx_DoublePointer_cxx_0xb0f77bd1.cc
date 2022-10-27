#include <jni.h>
#include <new>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_DoublePointer_1cxx_10xb0f77bd1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(double);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_DoublePointer_1cxx_10xb0f77bd1_native_1SetValue(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* value0 */) {
	(*reinterpret_cast<double*>(ptr)) = arg0;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_DoublePointer_1cxx_10xb0f77bd1_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<double*>(ptr);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_stdcxx_DoublePointer_1cxx_10xb0f77bd1_nativeToDouble(JNIEnv*, jclass, jlong ptr) {
	return (jdouble)(*reinterpret_cast<double*>(ptr));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_DoublePointer_1cxx_10xb0f77bd1_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new double());
}

#ifdef __cplusplus
}
#endif

#include <jni.h>
#include <new>
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_message_DoubleMsg_1cxx_10xc75dfe24__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::DoubleMsg);
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_graphscope_parallel_message_DoubleMsg_1cxx_10xc75dfe24_nativeGetData(JNIEnv*, jclass, jlong ptr) {
	return (jdouble)(reinterpret_cast<gs::DoubleMsg*>(ptr)->getData());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_message_DoubleMsg_1cxx_10xc75dfe24_nativeSetData(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* value0 */) {
	reinterpret_cast<gs::DoubleMsg*>(ptr)->setData(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_DoubleMsg_1cxx_10xc75dfe24_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::DoubleMsg());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_DoubleMsg_1cxx_10xc75dfe24_nativeCreateFactory1(JNIEnv*, jclass, jdouble arg0 /* inData0 */) {
	return reinterpret_cast<jlong>(new gs::DoubleMsg(arg0));
}

#ifdef __cplusplus
}
#endif

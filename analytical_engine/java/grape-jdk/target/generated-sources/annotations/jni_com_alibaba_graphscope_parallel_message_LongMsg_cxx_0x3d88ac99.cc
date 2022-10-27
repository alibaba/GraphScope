#include <jni.h>
#include <new>
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_message_LongMsg_1cxx_10x3d88ac99__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::LongMsg);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_LongMsg_1cxx_10x3d88ac99_nativeGetData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<gs::LongMsg*>(ptr)->getData());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_message_LongMsg_1cxx_10x3d88ac99_nativeSetData(JNIEnv*, jclass, jlong ptr, jlong arg0 /* value0 */) {
	reinterpret_cast<gs::LongMsg*>(ptr)->setData(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_LongMsg_1cxx_10x3d88ac99_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::LongMsg());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_LongMsg_1cxx_10x3d88ac99_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* inData0 */) {
	return reinterpret_cast<jlong>(new gs::LongMsg(arg0));
}

#ifdef __cplusplus
}
#endif

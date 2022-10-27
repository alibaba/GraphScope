#include <jni.h>
#include <new>
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_message_IntMsg_1cxx_10x5555e0eb__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::PrimitiveMessage<int32_t>);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_message_IntMsg_1cxx_10x5555e0eb_nativeGetData(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<gs::PrimitiveMessage<int32_t>*>(ptr)->getData());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_message_IntMsg_1cxx_10x5555e0eb_nativeSetData(JNIEnv*, jclass, jlong ptr, jint arg0 /* value0 */) {
	reinterpret_cast<gs::PrimitiveMessage<int32_t>*>(ptr)->setData(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_IntMsg_1cxx_10x5555e0eb_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::PrimitiveMessage<int32_t>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_message_IntMsg_1cxx_10x5555e0eb_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* inData0 */) {
	return reinterpret_cast<jlong>(new gs::PrimitiveMessage<int32_t>(arg0));
}

#ifdef __cplusplus
}
#endif

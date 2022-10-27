#include <jni.h>
#include <new>
#include "grape/parallel/message_in_buffer.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"
#include "core/java/java_messages.h"
#include "grape/parallel/message_in_buffer.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/type_alias.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::MessageInBuffer);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_MessageInBufferGen_1cxx_10x2b9fb1a1_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::MessageInBuffer());
}

#ifdef __cplusplus
}
#endif

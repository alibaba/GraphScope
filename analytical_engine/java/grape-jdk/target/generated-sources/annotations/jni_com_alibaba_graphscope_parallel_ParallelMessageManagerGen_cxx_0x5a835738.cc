#include <jni.h>
#include <new>
#include "grape/parallel/parallel_message_manager.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/graph/adj_list.h"
#include "core/java/java_messages.h"
#include "grape/parallel/parallel_message_manager.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/graph/adj_list.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::ParallelMessageManager);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1Finalize(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->Finalize();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1FinishARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->FinishARound();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1ForceContinue(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->ForceContinue();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1GetMsgSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::ParallelMessageManager*>(ptr)->GetMsgSize());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1Start(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->Start();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1StartARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->StartARound();
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_native_1ToTerminate(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<grape::ParallelMessageManager*>(ptr)->ToTerminate()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeGetMessageInBuffer(JNIEnv*, jclass, jlong ptr, jlong arg0 /* buf0 */) {
	return (reinterpret_cast<grape::ParallelMessageManager*>(ptr)->GetMessageInBuffer(*reinterpret_cast<grape::MessageInBuffer*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_ParallelMessageManagerGen_1cxx_10x5a835738_nativeInitChannels(JNIEnv*, jclass, jlong ptr, jint arg0 /* channel_num0 */) {
	reinterpret_cast<grape::ParallelMessageManager*>(ptr)->InitChannels(arg0);
}

#ifdef __cplusplus
}
#endif

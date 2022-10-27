#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/java_messages.h"
#include "core/java/graphx/graphx_fragment.h"
#include "grape/graph/adj_list.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/java_messages.h"
#include "core/java/graphx/graphx_fragment.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::DefaultMessageManager);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1Finalize(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->Finalize();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1FinishARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->FinishARound();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1ForceContinue(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->ForceContinue();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1GetMsgSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::DefaultMessageManager*>(ptr)->GetMsgSize());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1Start(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->Start();
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1StartARound(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::DefaultMessageManager*>(ptr)->StartARound();
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_parallel_DefaultMessageManagerGen_1cxx_10xb936493c_native_1ToTerminate(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<grape::DefaultMessageManager*>(ptr)->ToTerminate()) ? JNI_TRUE : JNI_FALSE;
}

#ifdef __cplusplus
}
#endif

#include <jni.h>
#include <new>
#include "grape/graph/adj_list.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_DestList_1cxx_10x69320d4b__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::DestList);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_DestList_1cxx_10x69320d4b_nativeBegin(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<grape::DestList*>(ptr)->begin);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_DestList_1cxx_10x69320d4b_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::DestList*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_DestList_1cxx_10x69320d4b_nativeEnd(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(reinterpret_cast<grape::DestList*>(ptr)->end);
}

#ifdef __cplusplus
}
#endif

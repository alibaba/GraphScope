#include <jni.h>
#include <new>
#include "grape/worker/comm_spec.h"
#include "grape/utils/bitset.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Bitset);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<grape::Bitset*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeCount(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<grape::Bitset*>(ptr)->count());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<grape::Bitset*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<grape::Bitset*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeGetBit(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	return (reinterpret_cast<grape::Bitset*>(ptr)->get_bit(arg0)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeGet_1word(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	return (jlong)(reinterpret_cast<grape::Bitset*>(ptr)->get_word(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeInit(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<grape::Bitset*>(ptr)->init(arg0);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativePartialEmpty(JNIEnv*, jclass, jlong ptr, jlong arg0 /* begin0 */, jlong arg1 /* end1 */) {
	return (reinterpret_cast<grape::Bitset*>(ptr)->partial_empty(arg0, arg1)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativePartial_1count(JNIEnv*, jclass, jlong ptr, jlong arg0 /* begin0 */, jlong arg1 /* end1 */) {
	return (jlong)(reinterpret_cast<grape::Bitset*>(ptr)->partial_count(arg0, arg1));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeResetBit(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	reinterpret_cast<grape::Bitset*>(ptr)->reset_bit(arg0);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeResetBitWithRet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	return (reinterpret_cast<grape::Bitset*>(ptr)->reset_bit_with_ret(arg0)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeSetBit(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	reinterpret_cast<grape::Bitset*>(ptr)->set_bit(arg0);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeSetBitWithRet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* i0 */) {
	return (reinterpret_cast<grape::Bitset*>(ptr)->set_bit_with_ret(arg0)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeSwap(JNIEnv*, jclass, jlong ptr, jlong arg0 /* other0 */) {
	reinterpret_cast<grape::Bitset*>(ptr)->swap(*reinterpret_cast<grape::Bitset*>(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_ds_Bitset_1cxx_10x2967d1e0_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new grape::Bitset());
}

#ifdef __cplusplus
}
#endif

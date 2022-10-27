#include <jni.h>
#include <new>
#include "vineyard/graph/fragment/arrow_fragment_group.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::ArrowFragmentGroup);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr);
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeEdgeLabelNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr)->edge_label_num());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeFragmentLocations(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr)->FragmentLocations()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeFragments(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr)->Fragments()));
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeTotalFragNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr)->total_frag_num());
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroup_1cxx_10xcc1dd3ca_nativeVertexLabelNum(JNIEnv*, jclass, jlong ptr) {
	return (jint)(reinterpret_cast<vineyard::ArrowFragmentGroup*>(ptr)->vertex_label_num());
}

#ifdef __cplusplus
}
#endif

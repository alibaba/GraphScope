#include <jni.h>
#include <new>
#include "core/java/graphx/fragment_getter.h"
#include <stdint.h>
#include <memory>
#include "vineyard/graph/fragment/arrow_fragment_group.h"
#include "vineyard/client/client.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroupGetter_1cxx_10xfcfb2a7d__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::ArrowFragmentGroupGetter);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroupGetter_1cxx_10xfcfb2a7d_nativeGet(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* client0 */, jlong arg1 /* groupId1 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::shared_ptr<vineyard::ArrowFragmentGroup>(reinterpret_cast<gs::ArrowFragmentGroupGetter*>(ptr)->Get(*reinterpret_cast<vineyard::Client*>(arg0), arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_fragment_ArrowFragmentGroupGetter_1cxx_10xfcfb2a7d_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new gs::ArrowFragmentGroupGetter());
}

#ifdef __cplusplus
}
#endif

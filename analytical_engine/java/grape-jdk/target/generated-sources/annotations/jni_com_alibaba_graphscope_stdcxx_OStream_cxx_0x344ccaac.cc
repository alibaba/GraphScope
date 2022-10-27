#include <jni.h>
#include <new>
#include <ostream>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_OStream_1cxx_10x344ccaac__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::ostream);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_OStream_1cxx_10x344ccaac_nativePut(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* c0 */) {
	reinterpret_cast<std::ostream*>(ptr)->put(arg0);
}

#ifdef __cplusplus
}
#endif

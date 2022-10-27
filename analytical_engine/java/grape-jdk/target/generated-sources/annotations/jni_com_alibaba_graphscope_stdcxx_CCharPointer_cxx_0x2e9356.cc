#include <jni.h>
#include <new>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_CCharPointer_1cxx_10x2e9356__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(char);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_stdcxx_CCharPointer_1cxx_10x2e9356_nativeToByte(JNIEnv*, jclass, jlong ptr) {
	return (jbyte)(*reinterpret_cast<char*>(ptr));
}

#ifdef __cplusplus
}
#endif

#include <jni.h>
#include <new>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1Iterator_1cxx_10xa5913122__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::string::iterator);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1Iterator_1cxx_10xa5913122_nativeCopy(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string::iterator(*reinterpret_cast<std::string::iterator*>(ptr)));
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1Iterator_1cxx_10xa5913122_nativeEq(JNIEnv*, jclass, jlong ptr, jlong arg0 /* rhs0 */) {
	return ((*reinterpret_cast<std::string::iterator*>(ptr)) == (*reinterpret_cast<std::string::iterator*>(arg0))) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1Iterator_1cxx_10xa5913122_nativeInc(JNIEnv*, jclass, jlong ptr) {
	return reinterpret_cast<jlong>(&(++(*reinterpret_cast<std::string::iterator*>(ptr))));
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1Iterator_1cxx_10xa5913122_nativeIndirection(JNIEnv*, jclass, jlong ptr) {
	return (jbyte)(*(*reinterpret_cast<std::string::iterator*>(ptr)));
}

#ifdef __cplusplus
}
#endif

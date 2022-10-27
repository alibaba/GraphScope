#include <jni.h>
#include <new>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::string);
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeByteAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jbyte)((*reinterpret_cast<std::string*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::string*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::string*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::string*>(ptr)->resize(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdString_1cxx_10xcec1e274_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::string());
}

#ifdef __cplusplus
}
#endif

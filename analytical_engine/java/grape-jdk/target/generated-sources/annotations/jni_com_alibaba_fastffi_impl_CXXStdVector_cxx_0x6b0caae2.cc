#include <jni.h>
#include <new>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<jbyte>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jbyte>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<jbyte>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jbyte>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeDispose(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<jbyte>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<std::vector<jbyte>*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jbyte)((*reinterpret_cast<std::vector<jbyte>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativePush_1back(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jbyte>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jbyte>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jbyte>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jbyte arg1 /* arg11 */) {
	(*reinterpret_cast<std::vector<jbyte>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jbyte>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<jbyte>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b0caae2_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new std::vector<jbyte>(arg0));
}

#ifdef __cplusplus
}
#endif

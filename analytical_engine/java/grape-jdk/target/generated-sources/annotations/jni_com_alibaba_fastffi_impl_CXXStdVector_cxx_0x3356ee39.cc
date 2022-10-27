#include <jni.h>
#include <new>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<jdouble>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jdouble>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<jdouble>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jdouble>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeDispose(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<jdouble>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<std::vector<jdouble>*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jdouble JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jdouble)((*reinterpret_cast<std::vector<jdouble>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativePush_1back(JNIEnv*, jclass, jlong ptr, jdouble arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jdouble>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jdouble>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jdouble>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jdouble arg1 /* arg11 */) {
	(*reinterpret_cast<std::vector<jdouble>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jdouble>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<jdouble>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x3356ee39_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new std::vector<jdouble>(arg0));
}

#ifdef __cplusplus
}
#endif

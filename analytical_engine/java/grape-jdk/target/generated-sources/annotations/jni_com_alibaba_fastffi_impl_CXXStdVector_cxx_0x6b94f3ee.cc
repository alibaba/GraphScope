#include <jni.h>
#include <new>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<jlong>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jlong>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<jlong>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jlong>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeDispose(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<jlong>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<std::vector<jlong>*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return (jlong)((*reinterpret_cast<std::vector<jlong>*>(ptr))[arg0]);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativePush_1back(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jlong>*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jlong>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<jlong>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	(*reinterpret_cast<std::vector<jlong>*>(ptr))[arg0] = arg1;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<jlong>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<jlong>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10x6b94f3ee_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new std::vector<jlong>(arg0));
}

#ifdef __cplusplus
}
#endif

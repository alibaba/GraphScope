#include <vector>
#include <jni.h>
#include <new>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::vector<std::vector<jint>>);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeCapacity(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->capacity());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->clear();
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeDispose(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::vector<std::vector<jint>>*>(ptr);
}

JNIEXPORT
jboolean JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeEmpty(JNIEnv*, jclass, jlong ptr) {
	return (reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->empty()) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeGet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(&((*reinterpret_cast<std::vector<std::vector<jint>>*>(ptr))[arg0]));
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativePush_1back(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->push_back(*reinterpret_cast<std::vector<jint>*>(arg0));
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeReserve(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->reserve(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */) {
	reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->resize(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeSet(JNIEnv*, jclass, jlong ptr, jlong arg0 /* arg00 */, jlong arg1 /* arg11 */) {
	(*reinterpret_cast<std::vector<std::vector<jint>>*>(ptr))[arg0] = *reinterpret_cast<std::vector<jint>*>(arg1);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::vector<std::vector<jint>>*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::vector<std::vector<jint>>());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_fastffi_impl_CXXStdVector_1cxx_10xbb6af271_nativeCreateFactory1(JNIEnv*, jclass, jint arg0 /* arg00 */) {
	return reinterpret_cast<jlong>(new std::vector<std::vector<jint>>(arg0));
}

#ifdef __cplusplus
}
#endif

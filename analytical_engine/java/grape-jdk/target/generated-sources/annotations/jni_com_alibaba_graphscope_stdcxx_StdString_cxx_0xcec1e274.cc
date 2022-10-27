#include <jni.h>
#include <new>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(std::string);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeAppend(JNIEnv*, jclass, jlong ptr, jlong arg0 /* rhs0 */) {
	return reinterpret_cast<jlong>(&(reinterpret_cast<std::string*>(ptr)->append(*reinterpret_cast<std::string*>(arg0))));
}

JNIEXPORT
jbyte JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeAt(JNIEnv*, jclass, jlong ptr, jlong arg0 /* index0 */) {
	return (jbyte)(reinterpret_cast<std::string*>(ptr)->at(arg0));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeBegin(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string::iterator(reinterpret_cast<std::string*>(ptr)->begin()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeC_1str(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->c_str());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeClear(JNIEnv*, jclass, jlong ptr) {
	reinterpret_cast<std::string*>(ptr)->clear();
}

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeCompare(JNIEnv*, jclass, jlong ptr, jlong arg0 /* str0 */) {
	return (jint)(reinterpret_cast<std::string*>(ptr)->compare(*reinterpret_cast<std::string*>(arg0)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeData(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->data());
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeDelete(JNIEnv*, jclass, jlong ptr) {
	delete reinterpret_cast<std::string*>(ptr);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeEnd(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string::iterator(reinterpret_cast<std::string*>(ptr)->end()));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* str0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find(*reinterpret_cast<std::string*>(arg0), arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind3(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* c0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find(arg0, arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind_1first_1of1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* str0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find_first_of(*reinterpret_cast<std::string*>(arg0), arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind_1first_1of3(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* c0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find_first_of(arg0, arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind_1last_1of1(JNIEnv*, jclass, jlong ptr, jlong arg0 /* str0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find_last_of(*reinterpret_cast<std::string*>(arg0), arg1));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeFind_1last_1of3(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* c0 */, jlong arg1 /* pos1 */) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->find_last_of(arg0, arg1));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativePush_1back(JNIEnv*, jclass, jlong ptr, jbyte arg0 /* c0 */) {
	reinterpret_cast<std::string*>(ptr)->push_back(arg0);
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeResize(JNIEnv*, jclass, jlong ptr, jlong arg0 /* size0 */) {
	reinterpret_cast<std::string*>(ptr)->resize(arg0);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeSize(JNIEnv*, jclass, jlong ptr) {
	return (jlong)(reinterpret_cast<std::string*>(ptr)->size());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeSubstr1(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* pos0 */, jlong arg1 /* len1 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<std::string*>(ptr)->substr(arg0, arg1)));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new std::string());
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_stdcxx_StdString_1cxx_10xcec1e274_nativeCreateFactory1(JNIEnv*, jclass, jlong arg0 /* string0 */) {
	return reinterpret_cast<jlong>(new std::string(*reinterpret_cast<std::string*>(arg0)));
}

#ifdef __cplusplus
}
#endif

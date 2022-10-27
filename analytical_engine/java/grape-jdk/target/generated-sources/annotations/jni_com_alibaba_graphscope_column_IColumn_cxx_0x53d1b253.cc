#include <string>
#include <jni.h>
#include <new>
#include "core/java/type_alias.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_column_IColumn_1cxx_10x53d1b253__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(gs::IColumn);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_column_IColumn_1cxx_10x53d1b253_nativeName(JNIEnv*, jclass, jlong ptr, jlong rv_base) {
	return reinterpret_cast<jlong>(new((void*)rv_base) std::string(reinterpret_cast<gs::IColumn*>(ptr)->name()));
}

JNIEXPORT
void JNICALL Java_com_alibaba_graphscope_column_IColumn_1cxx_10x53d1b253_nativeSetName(JNIEnv*, jclass, jlong ptr, jlong arg0 /* name0 */) {
	reinterpret_cast<gs::IColumn*>(ptr)->set_name(*reinterpret_cast<std::string*>(arg0));
}

#ifdef __cplusplus
}
#endif

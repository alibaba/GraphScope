#include <string>
#include <jni.h>
#include <new>
#include "vineyard/client/client.h"
#include "vineyard/common/util/json.h"
#include <map>

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_graphx_VineyardClient_1cxx_10xb9ed250f__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(vineyard::Client);
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardClient_1cxx_10xb9ed250f_nativeClusterInfo(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* meta0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) vineyard::Status(reinterpret_cast<vineyard::Client*>(ptr)->ClusterInfo(*reinterpret_cast<std::map<uint64_t,vineyard::json>*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardClient_1cxx_10xb9ed250f_nativeConnect(JNIEnv*, jclass, jlong ptr, jlong rv_base, jlong arg0 /* endPoint0 */) {
	return reinterpret_cast<jlong>(new((void*)rv_base) vineyard::Status(reinterpret_cast<vineyard::Client*>(ptr)->Connect(*reinterpret_cast<const std::string*>(arg0))));
}

JNIEXPORT
jlong JNICALL Java_com_alibaba_graphscope_graphx_VineyardClient_1cxx_10xb9ed250f_nativeCreateFactory0(JNIEnv*, jclass) {
	return reinterpret_cast<jlong>(new vineyard::Client());
}

#ifdef __cplusplus
}
#endif

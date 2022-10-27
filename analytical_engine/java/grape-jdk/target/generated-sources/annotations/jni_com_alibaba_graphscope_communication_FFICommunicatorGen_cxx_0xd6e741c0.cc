#include <jni.h>
#include <new>
#include "grape/communication/communicator.h"
#include "core/java/java_messages.h"
#include "grape/communication/communicator.h"
#include "core/java/java_messages.h"

#ifdef __cplusplus
extern "C" {
#endif

// Common Stubs

JNIEXPORT
jint JNICALL Java_com_alibaba_graphscope_communication_FFICommunicatorGen_1cxx_10xd6e741c0__1elementSize_00024_00024_00024(JNIEnv*, jclass) {
    return (jint)sizeof(grape::Communicator);
}

#ifdef __cplusplus
}
#endif

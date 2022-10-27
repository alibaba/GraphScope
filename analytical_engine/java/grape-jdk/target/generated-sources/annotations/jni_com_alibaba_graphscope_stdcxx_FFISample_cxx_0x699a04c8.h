#ifndef _JNI_COM_ALIBABA_GRAPHSCOPE_STDCXX_FFISAMPLE_CXX_0X699A04C8_H
#define _JNI_COM_ALIBABA_GRAPHSCOPE_STDCXX_FFISAMPLE_CXX_0X699A04C8_H
#include <utility>
#include <string>
#include <vector>
#include <jni.h>
#include <new>
namespace sample {
struct FFIMirrorSample {
	std::vector<jint> intVectorField;

	std::vector<jbyte> vectorBytes;

	std::vector<std::vector<jlong>> longVectorVectorField;

	std::vector<jdouble> doubleVectorField;

	std::vector<std::vector<jdouble>> doubleVectorVectorField;

	jint intField;

	std::vector<std::vector<jint>> intVectorVectorField;

	std::vector<jlong> longVectorField;

	std::string stringField;

	FFIMirrorSample()  : intVectorField(), vectorBytes(), longVectorVectorField(), doubleVectorField(), doubleVectorVectorField(), intField(0), intVectorVectorField(), longVectorField(), stringField() {}
	FFIMirrorSample(const FFIMirrorSample &from)  : intVectorField(from.intVectorField), vectorBytes(from.vectorBytes), longVectorVectorField(from.longVectorVectorField), doubleVectorField(from.doubleVectorField), doubleVectorVectorField(from.doubleVectorVectorField), intField(from.intField), intVectorVectorField(from.intVectorVectorField), longVectorField(from.longVectorField), stringField(from.stringField) {}
	FFIMirrorSample(FFIMirrorSample &&from)  : intVectorField(std::move(from.intVectorField)), vectorBytes(std::move(from.vectorBytes)), longVectorVectorField(std::move(from.longVectorVectorField)), doubleVectorField(std::move(from.doubleVectorField)), doubleVectorVectorField(std::move(from.doubleVectorVectorField)), intField(from.intField), intVectorVectorField(std::move(from.intVectorVectorField)), longVectorField(std::move(from.longVectorField)), stringField(std::move(from.stringField)) {}
	sample::FFIMirrorSample & operator = (const sample::FFIMirrorSample & from) {
		if (this == &from) return *this;
		intVectorField = from.intVectorField;
		vectorBytes = from.vectorBytes;
		longVectorVectorField = from.longVectorVectorField;
		doubleVectorField = from.doubleVectorField;
		doubleVectorVectorField = from.doubleVectorVectorField;
		intField = from.intField;
		intVectorVectorField = from.intVectorVectorField;
		longVectorField = from.longVectorField;
		stringField = from.stringField;
		return *this;
	}
	sample::FFIMirrorSample & operator = (sample::FFIMirrorSample && from) {
		if (this == &from) return *this;
		intVectorField = std::move(from.intVectorField);
		vectorBytes = std::move(from.vectorBytes);
		longVectorVectorField = std::move(from.longVectorVectorField);
		doubleVectorField = std::move(from.doubleVectorField);
		doubleVectorVectorField = std::move(from.doubleVectorVectorField);
		intField = from.intField;
		intVectorVectorField = std::move(from.intVectorVectorField);
		longVectorField = std::move(from.longVectorField);
		stringField = std::move(from.stringField);
		return *this;
	}
}; // end of type declaration
} // end namespace sample
#endif // _JNI_COM_ALIBABA_GRAPHSCOPE_STDCXX_FFISAMPLE_CXX_0X699A04C8_H


/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_

#ifdef ENABLE_JAVA_SDK

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"

#include "grape/grape.h"
#include "grape/util.h"

#include "core/java/javasdk.h"
#include "core/java/utils.h"

namespace gs {

// consistent with vineyard::TypeToInt
// 2=int32_t
// 4=int64_t
// 6=float
// 7=double
// 9=std::string(udf)

template <typename T,
          typename std::enable_if<std::is_same<T, grape::EmptyType>::value,
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building pod array with null builder";
  arrow::NullBuilder array_builder;
  int64_t total_length = 0;
  for (size_t i = 0; i < offset_arr.size(); ++i) {
    total_length += offset_arr[i].size();
  }
  array_builder.AppendNulls(total_length);

  array_builder.Finish(&array);
}
template <typename T,
          typename std::enable_if<(!std::is_same<T, std::string>::value &&
                                   !std::is_same<T, grape::EmptyType>::value),
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building pod array with pod builder";
  using elementType = T;
  using builderType = typename vineyard::ConvertToArrowType<T>::BuilderType;
  builderType array_builder;
  int64_t total_length = 0;
  for (size_t i = 0; i < offset_arr.size(); ++i) {
    total_length += offset_arr[i].size();
  }
  array_builder.Reserve(total_length);  // the number of elements

  for (size_t i = 0; i < data_arr.size(); ++i) {
    auto ptr = reinterpret_cast<const elementType*>(data_arr[i].data());
    auto cur_offset = offset_arr[i];

    for (size_t j = 0; j < cur_offset.size(); ++j) {
      array_builder.UnsafeAppend(*ptr);
      CHECK(sizeof(*ptr) == cur_offset[j]);
      ptr += 1;  // We have convert to T*, so plus 1 is ok.
    }
  }
  array_builder.Finish(&array);
}

template <typename T,
          typename std::enable_if<std::is_same<T, std::string>::value,
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building utf array with string builder";
  arrow::LargeStringBuilder array_builder;
  int64_t total_length = 0, total_bytes = 0;
  for (size_t i = 0; i < data_arr.size(); ++i) {
    total_bytes += data_arr[i].size();
    total_length += offset_arr[i].size();
  }
  array_builder.Reserve(total_length);  // the number of elements
  array_builder.ReserveData(total_bytes);

  for (size_t i = 0; i < data_arr.size(); ++i) {
    const char* ptr = data_arr[i].data();
    auto cur_offset = offset_arr[i];

    for (size_t j = 0; j < cur_offset.size(); ++j) {
      // for appending data to arrow_binary_builder, we use raw pointer to
      // avoid copy.
      array_builder.UnsafeAppend(ptr, cur_offset[j]);
      ptr += cur_offset[j];
    }
  }
  array_builder.Finish(&array);
}

static constexpr const char* JAVA_LOADER_CLASS =
    "com/alibaba/graphscope/loader/impl/FileLoader";
static constexpr const char* JAVA_LOADER_CREATE_METHOD = "create";
static constexpr const char* JAVA_LOADER_CREATE_SIG =
    "(Ljava/net/URLClassLoader;)Lcom/alibaba/graphscope/loader/impl/"
    "FileLoader;";
static constexpr const char* JAVA_LOADER_LOAD_VE_METHOD =
    "loadVerticesAndEdges";
static constexpr const char* JAVA_LOADER_LOAD_VE_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)I";
static constexpr const char* JAVA_LOADER_LOAD_E_METHOD = "loadEdges";
static constexpr const char* JAVA_LOADER_LOAD_E_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)V";
static constexpr const char* JAVA_LOADER_INIT_METHOD = "init";
static constexpr const char* JAVA_LOADER_INIT_SIG =
    "(IIILcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;)V";
static constexpr int GIRAPH_TYPE_CODE_LENGTH = 4;
class JavaLoaderInvoker {
 public:
  JavaLoaderInvoker() {
    load_thread_num = 1;
    if (getenv("LOADING_THREAD_NUM")) {
      load_thread_num = atoi(getenv("LOADING_THREAD_NUM"));
    }
    VLOG(1) << "loading thread num: " << load_thread_num;
    oids.resize(load_thread_num);
    vdatas.resize(load_thread_num);
    esrcs.resize(load_thread_num);
    edsts.resize(load_thread_num);
    edatas.resize(load_thread_num);

    oid_offsets.resize(load_thread_num);
    vdata_offsets.resize(load_thread_num);
    esrc_offsets.resize(load_thread_num);
    edst_offsets.resize(load_thread_num);
    edata_offsets.resize(load_thread_num);
    // Construct the FFIPointer

    if (!getenv("USER_JAR_PATH") || !getenv("GRAPE_JVM_OPTS")) {
      LOG(ERROR) << "expect env USER_JAR_PATH and GRAPE_JVM_OPTS set";
    }

    createFFIPointers();

    oid_type = vdata_type = edata_type = -1;
  }

  ~JavaLoaderInvoker() { VLOG(1) << "Destructing java loader invoker"; }

  void SetWorkerInfo(int worker_id, int worker_num) {
    VLOG(2) << "JavaLoaderInvoekr set worker Id, num " << worker_id << ", "
            << worker_num;
    worker_id_ = worker_id;
    worker_num_ = worker_num;
  }
  // load the class and call init method
  void InitJavaLoader() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);
      // construct java loader obj.
      jmethodID create_method = env->GetStaticMethodID(
          loader_class, JAVA_LOADER_CREATE_METHOD, JAVA_LOADER_CREATE_SIG);
      CHECK(create_method);

      java_loader_obj = env->NewGlobalRef(env->CallStaticObjectMethod(
          loader_class, create_method, gs_class_loader_obj));
      CHECK(java_loader_obj);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_INIT_METHOD, JAVA_LOADER_INIT_SIG);
      CHECK_NOTNULL(loader_method);

      env->CallVoidMethod(java_loader_obj, loader_method, worker_id_,
                          worker_num_, load_thread_num, oids_jobj, vdatas_jobj,
                          esrcs_jobj, edsts_jobj, edatas_jobj, oid_offsets_jobj,
                          vdata_offsets_jobj, esrc_offsets_jobj,
                          edst_offsets_jobj, edata_offsets_jobj);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Init java loader";
        return;
      }
    }
    VLOG(1) << "Successfully init java loader with params ";
  }
  void load_vertices_and_edges(const std::string& vertex_location,
                               const std::string vformatter) {
    VLOG(2) << "vertex file: " << vertex_location
            << ", formatter: " << vformatter;
    if (vformatter.find("giraph:") == std::string::npos) {
      LOG(ERROR) << "Expect a giraph formatter: giraph:your.class.name";
      return;
    }
    std::string vertex_location_prune = vertex_location;
    if (vertex_location.find("#") != std::string::npos) {
      vertex_location_prune =
          vertex_location.substr(0, vertex_location.find("#"));
    }
    std::string vformatter_class = vformatter.substr(7, std::string::npos);
    int giraph_type_int = callJavaLoaderVertices(vertex_location_prune.c_str(),
                                                 vformatter_class.c_str());
    CHECK_GE(giraph_type_int, 0);

    // fetch giraph graph types infos, so we can optimizing graph store by use
    // primitive types for LongWritable.
    parseGiraphTypeInt(giraph_type_int);
  }

  // load vertices must be called before load edge, since we assume giraph type
  // int has been calculated.
  void load_edges(const std::string& edge_location,
                  const std::string eformatter) {
    VLOG(2) << "edge file: " << edge_location << " eformatter: " << eformatter;
    if (eformatter.find("giraph:") == std::string::npos) {
      LOG(ERROR) << "Expect a giraph formatter: giraph:your.class.name";
      return;
    }
    std::string edge_location_prune = edge_location;
    if (edge_location.find("#") != std::string::npos) {
      edge_location_prune = edge_location.substr(0, edge_location.find("#"));
    }
    std::string eformatter_class = eformatter.substr(7, std::string::npos);

    callJavaLoaderEdges(edge_location_prune.c_str(), eformatter_class.c_str());
  }

  std::shared_ptr<arrow::Table> get_edge_table() {
    CHECK(oid_type > 0 && edata_type > 0);
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t esrc_total_length = 0, edst_total_length = 0,
            edata_total_length = 0;
    int64_t esrc_total_bytes = 0, edst_total_bytes = 0, edata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      esrc_total_length += esrc_offsets[i].size();
      edst_total_length += edst_offsets[i].size();
      edata_total_length += edata_offsets[i].size();

      esrc_total_bytes += esrcs[i].size();
      edst_total_bytes += edsts[i].size();
      edata_total_bytes += edatas[i].size();
    }
    CHECK((esrc_total_length == edst_total_length) &&
          (edst_total_length == edata_total_length));
    VLOG(10) << "worker " << worker_id_ << " Building edge table "
             << " esrc len: [" << esrc_total_length << "] esrc total bytes: ["
             << esrc_total_bytes << "] edst len: [" << edst_total_length
             << "] esrc total bytes: [" << edst_total_bytes << "] edata len: ["
             << edata_total_length << "] edata total bytes: ["
             << edata_total_bytes << "]";

    double edgeTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> esrc_array, edst_array, edata_array;

    buildArray(oid_type, esrc_array, esrcs, esrc_offsets);
    buildArray(oid_type, edst_array, edsts, edst_offsets);
    buildArray(edata_type, edata_array, edatas, edata_offsets);

    VLOG(10) << "Finish edge array building esrc: " << esrc_array->ToString()
             << " edst: " << edst_array->ToString()
             << " edata: " << edata_array->ToString();

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("src", getArrowDataType(oid_type)),
                       arrow::field("dst", getArrowDataType(oid_type)),
                       arrow::field("data", getArrowDataType(edata_type))});

    auto res =
        arrow::Table::Make(schema, {esrc_array, edst_array, edata_array});
    VLOG(10) << "worker " << worker_id_
             << " generated table, rows:" << res->num_rows()
             << " cols: " << res->num_columns();

    edgeTableBuildingTime += grape::GetCurrentTime();
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table cost: " << edgeTableBuildingTime;
    return res;
  }

  std::shared_ptr<arrow::Table> get_vertex_table() {
    CHECK(oid_type > 0 && vdata_type > 0);
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t oid_length = 0;
    int64_t oid_total_bytes = 0;
    int64_t vdata_total_length = 0;
    int64_t vdata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      oid_length += oid_offsets[i].size();
      vdata_total_length += vdata_offsets[i].size();
      oid_total_bytes += oids[i].size();
      vdata_total_bytes += vdatas[i].size();
    }
    CHECK(oid_length == vdata_total_length);
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table from oid array of size [" << oid_length
             << "] oid total bytes: [" << oid_total_bytes << "] vdata size: ["
             << vdata_total_length << "] total bytes: [" << vdata_total_bytes
             << "]";

    double vertexTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> oid_array;
    std::shared_ptr<arrow::Array> vdata_array;

    buildArray(oid_type, oid_array, oids, oid_offsets);
    buildArray(vdata_type, vdata_array, vdatas, vdata_offsets);

    VLOG(10) << "Finish vertex array building oid array: "
             << oid_array->ToString() << " vdata: " << vdata_array->ToString();

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("oid", getArrowDataType(oid_type)),
                       arrow::field("vdata", getArrowDataType(vdata_type))});

    auto res = arrow::Table::Make(schema, {oid_array, vdata_array});
    VLOG(10) << "worker " << worker_id_
             << " generated table, rows:" << res->num_rows()
             << " cols: " << res->num_columns();

    vertexTableBuildingTime += grape::GetCurrentTime();
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table cost: " << vertexTableBuildingTime;
    return res;
  }

 private:
  std::shared_ptr<arrow::DataType> getArrowDataType(int data_type) {
    if (data_type == 2) {
      return vineyard::ConvertToArrowType<int32_t>::TypeValue();
    } else if (data_type == 4) {
      return vineyard::ConvertToArrowType<int64_t>::TypeValue();
    } else if (data_type == 6) {
      return vineyard::ConvertToArrowType<float>::TypeValue();
    } else if (data_type == 7) {
      return vineyard::ConvertToArrowType<double>::TypeValue();
    } else if (data_type == 9) {
      return vineyard::ConvertToArrowType<std::string>::TypeValue();
    } else if (data_type == 1) {
      return arrow::null();
    } else {
      LOG(ERROR) << "Wrong data type: " << data_type;
      return arrow::null();
    }
  }
  void buildArray(int data_type, std::shared_ptr<arrow::Array>& array,
                  const std::vector<std::vector<char>>& data_arr,
                  const std::vector<std::vector<int>>& offset_arr) {
    if (data_type == 2) {
      BuildArray<int32_t>(array, data_arr, offset_arr);
    } else if (data_type == 4) {
      BuildArray<int64_t>(array, data_arr, offset_arr);
    } else if (data_type == 6) {
      BuildArray<float>(array, data_arr, offset_arr);
    } else if (data_type == 7) {
      BuildArray<double>(array, data_arr, offset_arr);
    } else if (data_type == 9) {
      BuildArray<std::string>(array, data_arr, offset_arr);
    } else if (data_type == 1) {
      BuildArray<grape::EmptyType>(array, data_arr, offset_arr);
    } else {
      LOG(ERROR) << "Wrong data type: " << data_type;
    }
  }
  void createFFIPointers() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      if (!getenv("USER_JAR_PATH")) {
        LOG(ERROR) << "expect env USER_JAR_PATH set";
      }
      std::string user_jar_path = getenv("USER_JAR_PATH");

      gs_class_loader_obj = gs::CreateClassLoader(env, user_jar_path);
      CHECK_NOTNULL(gs_class_loader_obj);
      {
        oids_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oids));
        vdatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdatas));
        esrcs_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrcs));
        edsts_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edsts));
        edatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edatas));
      }
      {
        oid_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oid_offsets));
        vdata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdata_offsets));
        esrc_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrc_offsets));
        edst_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edst_offsets));
        edata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edata_offsets));
      }
    }
    VLOG(1) << "Finish creating ffi wrappers";
  }

  int callJavaLoaderVertices(const char* file_path, const char* java_params) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_LOAD_VE_METHOD, JAVA_LOADER_LOAD_VE_SIG);
      CHECK_NOTNULL(loader_method);

      jstring file_path_jstring = env->NewStringUTF(file_path);
      jstring java_params_jstring = env->NewStringUTF(java_params);
      double javaLoadingTime = -grape::GetCurrentTime();

      jint res = env->CallIntMethod(java_loader_obj, loader_method,
                                    file_path_jstring, java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return -1;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1) << "Successfully Loaded graph vertex data from Java loader, "
                 "duration: "
              << javaLoadingTime;
      return res;
    } else {
      LOG(ERROR) << "Java env not available.";
      return -1;
    }
  }

  int callJavaLoaderEdges(const char* file_path, const char* java_params) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_LOAD_E_METHOD, JAVA_LOADER_LOAD_E_SIG);
      CHECK_NOTNULL(loader_method);

      jstring file_path_jstring = env->NewStringUTF(file_path);
      jstring java_params_jstring = env->NewStringUTF(java_params);
      double javaLoadingTime = -grape::GetCurrentTime();

      env->CallVoidMethod(java_loader_obj, loader_method, file_path_jstring,
                          java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return -1;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1)
          << "Successfully Loaded graph edge data from Java loader, duration: "
          << javaLoadingTime;
      return 0;
    } else {
      LOG(ERROR) << "Java env not available.";
      return -1;
    }
  }

  void parseGiraphTypeInt(int giraph_type_int) {
    edata_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH;
    vdata_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH;
    oid_type = (giraph_type_int & 0x000F);
    giraph_type_int = giraph_type_int >> GIRAPH_TYPE_CODE_LENGTH;
    CHECK_EQ(giraph_type_int, 0);
    VLOG(1) << "giraph types: " << oid_type << vdata_type << edata_type;
  }

  int worker_id_, worker_num_, load_thread_num;
  int oid_type, vdata_type, edata_type;
  std::vector<std::vector<char>> oids;
  std::vector<std::vector<char>> vdatas;
  std::vector<std::vector<char>> esrcs;
  std::vector<std::vector<char>> edsts;
  std::vector<std::vector<char>> edatas;

  std::vector<std::vector<int>> oid_offsets;
  std::vector<std::vector<int>> vdata_offsets;
  std::vector<std::vector<int>> esrc_offsets;
  std::vector<std::vector<int>> edst_offsets;
  std::vector<std::vector<int>> edata_offsets;

  jobject gs_class_loader_obj;
  jobject java_loader_obj;

  jobject oids_jobj;
  jobject vdatas_jobj;
  jobject esrcs_jobj;
  jobject edsts_jobj;
  jobject edatas_jobj;

  jobject oid_offsets_jobj;
  jobject vdata_offsets_jobj;
  jobject esrc_offsets_jobj;
  jobject edst_offsets_jobj;
  jobject edata_offsets_jobj;
};
};  // namespace gs

#endif

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_

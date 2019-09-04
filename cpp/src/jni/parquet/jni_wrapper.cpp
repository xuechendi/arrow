// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <jni.h>
#include <string>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/ipc/api.h>
#include <iostream>

#include "concurrent_map.h"
#include "ParquetReader.h"

static jclass arrow_record_batch_builder_class;
static jmethodID arrow_record_batch_builder_constructor;

static jclass arrow_field_node_builder_class;
static jmethodID arrow_field_node_builder_constructor;

static jclass arrowbuf_builder_class;
static jmethodID arrowbuf_builder_constructor;

using arrow::jni::ConcurrentMap;
static ConcurrentMap<std::shared_ptr<arrow::Buffer>> buffer_holder_;

static jclass io_exception_class;
static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;

static jint JNI_VERSION = JNI_VERSION_1_8;

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  if (global_class == nullptr) {
    exit(-1);
  }
  return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  //std::cerr << this_class << "(" << name << ", " << sig << ")"  << std::endl;
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    std::cerr << error_message << std::endl;
    exit(-1);
  }

  return ret;
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  std::vector<char> buffer(clen);
  env->GetStringUTFRegion(string, 0, jlen, buffer.data());
  return std::string(buffer.data(), clen);
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  illegal_access_exception_class =
    CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
    CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  arrow_record_batch_builder_class =
    CreateGlobalClassReference(env, "Lorg/apache/arrow/adapter/builder/ArrowRecordBatchBuilder;");
  arrow_record_batch_builder_constructor = GetMethodID(env, arrow_record_batch_builder_class,
    "<init>",
    "(I[Lorg/apache/arrow/adapter/builder/ArrowFieldNodeBuilder;[Lorg/apache/arrow/adapter/builder/ArrowBufBuilder;)V");

  arrow_field_node_builder_class =
    CreateGlobalClassReference(env, "Lorg/apache/arrow/adapter/builder/ArrowFieldNodeBuilder;");
  arrow_field_node_builder_constructor = GetMethodID(env, arrow_field_node_builder_class, "<init>", "(II)V");

  arrowbuf_builder_class =
    CreateGlobalClassReference(env, "Lorg/apache/arrow/adapter/builder/ArrowBufBuilder;");
  arrowbuf_builder_constructor = GetMethodID(env, arrowbuf_builder_class, "<init>", "(JJIJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(arrow_field_node_builder_class);
  env->DeleteGlobalRef(arrowbuf_builder_class);
  env->DeleteGlobalRef(arrow_record_batch_builder_class);

  buffer_holder_.Clear();
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeOpenHdfsReader
  (JNIEnv *env, jobject obj, jstring path) {
  std::string cpath = JStringToCString(env, path);
  jni::parquet::HdfsConnector* reader = new jni::parquet::HdfsConnector(cpath);
  return (long)reader;
}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeOpenParquetReader
  (JNIEnv *env, jobject obj, jlong hdfsReaderHandler, jintArray column_indices, jintArray row_group_indices, jlong batch_size) {
  jni::parquet::HdfsConnector *hdfsReader = (jni::parquet::HdfsConnector*)hdfsReaderHandler;

  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr, column_indices_ptr + column_indices_len);

  int row_group_indices_len = env->GetArrayLength(row_group_indices);
  if (row_group_indices_len == 0) {
    std::vector<int> _row_group_indices = {};
    jni::parquet::ParquetReader* reader =
      new jni::parquet::ParquetReader(hdfsReader, _row_group_indices, _column_indices, batch_size);
    return (long)reader;
  } else {
    jint* row_group_indices_ptr = env->GetIntArrayElements(row_group_indices, 0);
    std::vector<int> _row_group_indices(row_group_indices_ptr, row_group_indices_ptr + row_group_indices_len);
    jni::parquet::ParquetReader* reader =
      new jni::parquet::ParquetReader(hdfsReader, _row_group_indices, _column_indices, batch_size);
    return (long)reader;
  }

}

JNIEXPORT jlong JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeOpenParquetReaderWithRange
  (JNIEnv *env, jobject obj, jlong hdfsReaderHandler, jintArray column_indices, jlong start_pos, jlong end_pos, jlong batch_size) {
  jni::parquet::HdfsConnector *hdfsReader = (jni::parquet::HdfsConnector*)hdfsReaderHandler;

  int column_indices_len = env->GetArrayLength(column_indices);
  jint* column_indices_ptr = env->GetIntArrayElements(column_indices, 0);
  std::vector<int> _column_indices(column_indices_ptr, column_indices_ptr + column_indices_len);

  jni::parquet::ParquetReader* reader =
    new jni::parquet::ParquetReader(hdfsReader, _column_indices, start_pos, end_pos, batch_size);
  return (long)reader;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeCloseHdfsReader
  (JNIEnv *env, jobject obj, jlong reader_ptr) {
  jni::parquet::HdfsConnector* reader =
    (jni::parquet::HdfsConnector*)reader_ptr;
  delete reader; 
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeCloseParquetReader
  (JNIEnv *env, jobject obj, jlong reader_ptr) {
  jni::parquet::ParquetReader* reader =
    (jni::parquet::ParquetReader*)reader_ptr;
  delete reader; 
}

JNIEXPORT jobject JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeReadNext
  (JNIEnv *env, jobject obj, jlong reader_ptr) {
  std::shared_ptr<::arrow::RecordBatch> record_batch;
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;
  arrow::Status status = reader->readNext(&record_batch);

  if (!status.ok() || !record_batch) {
    return nullptr;
  }

  auto schema = reader->schema();

  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), arrow_field_node_builder_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(arrow_field_node_builder_class, arrow_field_node_builder_constructor,
                                   column->length(), column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray arrowbuf_builder_array =
      env->NewObjectArray(buffers.size(), arrowbuf_builder_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    jobject arrowBufBuilder = env->NewObject(arrowbuf_builder_class, arrowbuf_builder_constructor,
                                    buffer_holder_.Insert(buffer), buffer->data(),
                                    (int)buffer->size(), buffer->capacity());
    env->SetObjectArrayElement(arrowbuf_builder_array, j, arrowBufBuilder);
  }

  // create RecordBatch
  jobject arrowRecordBatchBuilder = env->NewObject(arrow_record_batch_builder_class,
                                            arrow_record_batch_builder_constructor,
                                            record_batch->num_rows(),
                                            field_array, arrowbuf_builder_array);
  return arrowRecordBatchBuilder;
}

JNIEXPORT jobject JNICALL Java_org_apache_arrow_adapter_builder_ParquetReaderHandler_nativeGetSchema
  (JNIEnv *env, jobject obj, jlong reader_ptr) {
  jni::parquet::ParquetReader* reader = (jni::parquet::ParquetReader*)reader_ptr;
  std::shared_ptr<::arrow::Schema> schema = reader->schema();
  std::shared_ptr<arrow::Buffer> out;
  arrow::Status status =
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool(), &out);
  if (!status.ok()) {
    return nullptr;
  }

  jbyteArray ret = env->NewByteArray(out->size());
  auto src = reinterpret_cast<const jbyte*>(out->data());
  env->SetByteArrayRegion(ret, 0, out->size(), src);
  return ret;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_adapter_builder_AdaptorReferenceManager_nativeRelease(
    JNIEnv* env, jobject this_obj, jlong id) {
  buffer_holder_.Erase(id);
}

#ifdef __cplusplus
}
#endif

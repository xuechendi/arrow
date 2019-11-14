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

#include "arrow/compute/kernels/hash.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/dict_internal.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

using internal::checked_cast;
using internal::DictionaryTraits;
using internal::HashTraits;

namespace compute {

namespace {

#define CHECK_IMPLEMENTED(KERNEL, FUNCNAME, TYPE)                                       \
  if (!KERNEL) {                                                                        \
    return Status::NotImplemented(FUNCNAME, " not implemented for ", type->ToString()); \
  }

// ----------------------------------------------------------------------
// Unique implementation

class ActionBase {
 public:
  ActionBase(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool) {}

 protected:
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
};

class UniqueAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  Status Reset() { return Status::OK(); }

  Status Reserve(const int64_t length) { return Status::OK(); }

  template <class Index>
  void ObserveNullFound(Index index) {}

  template <class Index>
  void ObserveNullNotFound(Index index) {}

  template <class Index>
  void ObserveFound(Index index) {}

  template <class Index>
  void ObserveNotFound(Index index) {}

  Status Flush(Datum* out) { return Status::OK(); }

  std::shared_ptr<DataType> out_type() const { return type_; }

  Status FlushFinal(Datum* out) { return Status::OK(); }
};

// ----------------------------------------------------------------------
// Count values implementation (see HashKernel for description of methods)

class ValueCountsAction final : ActionBase {
 public:
  using ActionBase::ActionBase;

  ValueCountsAction(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ActionBase(type, pool), count_builder_(pool) {}

  Status Reserve(const int64_t length) {
    // builder size is independent of input array size.
    return Status::OK();
  }

  Status Reset() {
    count_builder_.Reset();
    return Status::OK();
  }

  // Don't do anything on flush because we don't want to finalize the builder
  // or incur the cost of memory copies.
  Status Flush(Datum* out) { return Status::OK(); }

  std::shared_ptr<DataType> out_type() const { return type_; }

  // Return the counts corresponding the MemoTable keys.
  Status FlushFinal(Datum* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(count_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  template <class Index>
  Status CheckOrAppend(Index index) {
    Status s = Status::OK();
    Index to_append_size = index - count_builder_.length() + 1;
    if (to_append_size > 0) {
      for (Index i = 0; i < to_append_size; i++) {
        s = count_builder_.Append(0);
      }
    }
    return s;
  }

  template <class Index>
  void ObserveNullFound(Index index) {
    auto s = CheckOrAppend(index);
    count_builder_[index]++;
  }

  template <class Index>
  void ObserveNullNotFound(Index index) {
    ARROW_LOG(FATAL) << "ObserveNullNotFound without err_status should not be called";
  }

  template <class Index>
  void ObserveNullNotFound(Index index, Status* status) {
    auto s = CheckOrAppend(index);
    if (ARROW_PREDICT_FALSE(!s.ok())) {
      *status = s;
    }
  }

  template <class Index>
  void ObserveFound(Index slot) {
    auto s = CheckOrAppend(slot);
    count_builder_[slot]++;
  }

  template <class Index>
  void ObserveNotFound(Index slot, Status* status) {
    auto s = CheckOrAppend(slot);
    count_builder_[slot]++;
    if (ARROW_PREDICT_FALSE(!s.ok())) {
      *status = s;
    }
  }

 private:
  Int64Builder count_builder_;
};

// ----------------------------------------------------------------------
// Dictionary encode implementation (see HashKernel for description of methods)

class DictEncodeAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  DictEncodeAction(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ActionBase(type, pool), indices_builder_(pool) {}

  Status Reset() {
    indices_builder_.Reset();
    return Status::OK();
  }

  Status Reserve(const int64_t length) { return indices_builder_.Reserve(length); }

  template <class Index>
  void ObserveNullFound(Index index) {
    indices_builder_.UnsafeAppendNull();
  }

  template <class Index>
  void ObserveNullNotFound(Index index) {
    indices_builder_.UnsafeAppendNull();
  }

  template <class Index>
  void ObserveFound(Index index) {
    indices_builder_.UnsafeAppend(index);
  }

  template <class Index>
  void ObserveNotFound(Index index) {
    ObserveFound(index);
  }

  Status Flush(Datum* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(indices_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return int32(); }
  Status FlushFinal(Datum* out) { return Status::OK(); }

 private:
  Int32Builder indices_builder_;
};

// ----------------------------------------------------------------------
// Base class for all hash kernel implementations

class HashKernel : public UnaryKernel {
 public:
  // Reset for another run.
  virtual Status Reset() = 0;
  // Prepare the Action for the given input (e.g. reserve appropriately sized
  // data structures) and visit the given input with Action.
  virtual Status Append(FunctionContext* ctx, const ArrayData& input) = 0;
  // Flush out accumulated results from the last invocation of Call.
  virtual Status Flush(Datum* out) = 0;
  // Flush out accumulated results across all invocations of Call. The kernel
  // should not be used until after Reset() is called.
  virtual Status FlushFinal(Datum* out) = 0;
  // Get the values (keys) acummulated in the dictionary so far.
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;
};

class HashKernelImpl : public HashKernel {
 public:
  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());
    RETURN_NOT_OK(Append(ctx, *input.array()));
    return Flush(out);
  }

  Status Append(FunctionContext* ctx, const ArrayData& input) override {
    std::lock_guard<std::mutex> guard(lock_);
    return Append(input);
  }

  virtual Status Append(const ArrayData& arr) = 0;

 protected:
  std::mutex lock_;
};

// ----------------------------------------------------------------------
// Base class for all "regular" hash kernel implementations
// (NullType has a separate implementation)

template <bool B, typename T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

template <typename Type, typename Scalar, typename Action, bool with_error_status = false,
          bool with_memo_visit_null = true,
          typename MemoTableType = std::shared_ptr<internal::MemoTable>>
class RegularHashKernelImpl : public HashKernelImpl {
 public:
  RegularHashKernelImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                        std::shared_ptr<MemoTableType> memo_table)
      : pool_(pool), type_(type), action_(type, pool) {
    memo_table_ = memo_table;
  }

  Status Reset() override {
    // memo_table_.reset(new MemoTableType(pool_, 0));
    return action_.Reset();
  }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    return ArrayDataVisitor<Type>::Visit(arr, this);
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }

  Status FlushFinal(Datum* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return DictionaryTraits<Type>::GetDictionaryArrayData(
        pool_, type_, *memo_table_.get(), 0 /* start_offset */, out);
  }

  template <typename Enable = Status>
  auto VisitNull() -> enable_if_t<!with_error_status, Enable> {
    auto on_found = [this](int32_t memo_index) { action_.ObserveNullFound(memo_index); };
    auto on_not_found = [this](int32_t memo_index) {
      action_.ObserveNullNotFound(memo_index);
    };

    if (with_memo_visit_null) {
      memo_table_->GetOrInsertNull(on_found, on_not_found);
    } else {
      action_.ObserveNullNotFound(-1);
    }
    return Status::OK();
  }

  template <typename Enable = Status>
  auto VisitNull() -> enable_if_t<with_error_status, Enable> {
    Status s = Status::OK();
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this, &s](int32_t memo_index) {
      action_.ObserveNotFound(memo_index, &s);
    };

    if (with_memo_visit_null) {
      memo_table_->GetOrInsertNull(on_found, on_not_found);
    } else {
      action_.ObserveNullNotFound(-1);
    }

    return s;
  }

  template <typename Enable = Status>
  auto VisitValue(const Scalar& value) ->
      typename std::enable_if<!with_error_status, Enable>::type {
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this](int32_t memo_index) {
      action_.ObserveNotFound(memo_index);
    };

    memo_table_->GetOrInsert(value, on_found, on_not_found);
    return Status::OK();
  }

  template <typename Enable = Status>
  auto VisitValue(const Scalar& value) ->
      typename std::enable_if<with_error_status, Enable>::type {
    Status s = Status::OK();
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this, &s](int32_t memo_index) {
      action_.ObserveNotFound(memo_index, &s);
    };
    memo_table_->GetOrInsert(value, on_found, on_not_found);
    return s;
  }

  std::shared_ptr<DataType> out_type() const override { return action_.out_type(); }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
  std::shared_ptr<MemoTableType> memo_table_;
};

// ----------------------------------------------------------------------
// Hash kernel implementation for nulls

template <typename Action, typename MemoTableType>
class NullHashKernelImpl : public HashKernelImpl {
 public:
  NullHashKernelImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                     std::shared_ptr<MemoTableType> memo_table)
      : pool_(pool), type_(type), action_(type, pool) {}

  Status Reset() override { return action_.Reset(); }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    for (int64_t i = 0; i < arr.length; ++i) {
      if (i == 0) {
        action_.ObserveNullNotFound(0);
      } else {
        action_.ObserveNullFound(0);
      }
    }
    return Status::OK();
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }
  Status FlushFinal(Datum* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being a valid dictionary value
    auto null_array = std::make_shared<NullArray>(0);
    *out = null_array->data();
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return null(); }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType, typename Enable = void>
struct HashKernelTraits {};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        MemoTableType, enable_if_null<Type>> {
  using HashKernelImpl = NullHashKernelImpl<Action, MemoTableType>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        MemoTableType, enable_if_has_c_type<Type>> {
  using HashKernelImpl =
      RegularHashKernelImpl<Type, typename Type::c_type, Action, with_error_status,
                            with_memo_visit_null, MemoTableType>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        MemoTableType, enable_if_boolean<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, bool, Action, with_error_status,
                                               with_memo_visit_null, MemoTableType>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        MemoTableType, enable_if_binary<Type>> {
  using HashKernelImpl =
      RegularHashKernelImpl<Type, util::string_view, Action, with_error_status,
                            with_memo_visit_null, MemoTableType>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename MemoTableType>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        MemoTableType, enable_if_fixed_size_binary<Type>> {
  using HashKernelImpl =
      RegularHashKernelImpl<Type, util::string_view, Action, with_error_status,
                            with_memo_visit_null, MemoTableType>;
};

}  // namespace

template <typename InType, typename MemoTableType>
Status GetUniqueKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                       std::shared_ptr<MemoTableType> memo_table,
                       std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;
  kernel.reset(
      new
      typename HashKernelTraits<InType, UniqueAction, false, true,
                                MemoTableType>::HashKernelImpl(type, ctx->memory_pool(),
                                                               memo_table));

  CHECK_IMPLEMENTED(kernel, "unique", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

template <typename InType, typename MemoTableType>
Status GetDictionaryEncodeKernel(FunctionContext* ctx,
                                 const std::shared_ptr<DataType>& type,
                                 std::shared_ptr<MemoTableType> memo_table,
                                 std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;
  kernel.reset(
      new
      typename HashKernelTraits<InType, DictEncodeAction, false, false,
                                MemoTableType>::HashKernelImpl(type, ctx->memory_pool(),
                                                               memo_table));

  CHECK_IMPLEMENTED(kernel, "dictionary-encode", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

template <typename InType, typename MemoTableType>
Status GetValueCountsKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                            std::shared_ptr<MemoTableType> memo_table,
                            std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;
  kernel.reset(
      new
      typename HashKernelTraits<InType, ValueCountsAction, true, true,
                                MemoTableType>::HashKernelImpl(type, ctx->memory_pool(),
                                                               memo_table));

  CHECK_IMPLEMENTED(kernel, "count-values", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

namespace {

Status InvokeHash(FunctionContext* ctx, HashKernel* func, const Datum& value,
                  std::vector<Datum>* kernel_outputs,
                  std::shared_ptr<Array>* dictionary) {
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, value, kernel_outputs));

  std::shared_ptr<ArrayData> dict_data;
  RETURN_NOT_OK(func->GetDictionary(&dict_data));
  *dictionary = MakeArray(dict_data);
  return Status::OK();
}

}  // namespace

template <typename InType, typename MemoTableType>
Status Unique(FunctionContext* ctx, const Datum& value,
              std::shared_ptr<MemoTableType> memo_table, std::shared_ptr<Array>* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetUniqueKernel<InType>(ctx, value.type(), memo_table, &func));

  std::vector<Datum> dummy_outputs;
  return InvokeHash(ctx, func.get(), value, &dummy_outputs, out);
}

template <typename InType, typename MemoTableType>
Status DictionaryEncode(FunctionContext* ctx, const Datum& value,
                        std::shared_ptr<MemoTableType> memo_table, Datum* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetDictionaryEncodeKernel<InType>(ctx, value.type(), memo_table, &func));

  std::shared_ptr<Array> dictionary;
  std::vector<Datum> indices_outputs;
  RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &indices_outputs, &dictionary));

  // Wrap indices in dictionary arrays for result
  std::vector<std::shared_ptr<Array>> dict_chunks;
  std::shared_ptr<DataType> dict_type;

  if (indices_outputs.size() == 0) {
    // Special case: empty was an empty chunked array
    DCHECK_EQ(value.kind(), Datum::CHUNKED_ARRAY);
    dict_type = ::arrow::dictionary(int32(), dictionary->type());
    *out = std::make_shared<ChunkedArray>(dict_chunks, dict_type);
  } else {
    // Create the dictionary type
    DCHECK_EQ(indices_outputs[0].kind(), Datum::ARRAY);
    dict_type = ::arrow::dictionary(indices_outputs[0].array()->type, dictionary->type());

    // Create DictionaryArray for each piece yielded by the kernel invocations
    for (const Datum& datum : indices_outputs) {
      dict_chunks.emplace_back(std::make_shared<DictionaryArray>(
          dict_type, MakeArray(datum.array()), dictionary));
    }
    *out = detail::WrapArraysLike(value, dict_chunks);
  }

  return Status::OK();
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;

template <typename InType, typename MemoTableType>
Status ValueCounts(FunctionContext* ctx, const Datum& value,
                   std::shared_ptr<MemoTableType> memo_table,
                   std::shared_ptr<Array>* counts) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetValueCountsKernel<InType>(ctx, value.type(), memo_table, &func));

  // Calls return nothing for counts.
  std::vector<Datum> unused_output;
  std::shared_ptr<Array> uniques;
  RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &unused_output, &uniques));

  Datum value_counts;
  RETURN_NOT_OK(func->FlushFinal(&value_counts));

  auto data_type = std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>{
      std::make_shared<Field>(kValuesFieldName, uniques->type()),
      std::make_shared<Field>(kCountsFieldName, int64())});
  *counts = std::make_shared<StructArray>(
      data_type, uniques->length(),
      std::vector<std::shared_ptr<Array>>{uniques, MakeArray(value_counts.array())});
  return Status::OK();
}

#define PROCESS_SUPPORTED_TYPES(PROCESS) \
  PROCESS(BooleanType)                   \
  PROCESS(UInt8Type)                     \
  PROCESS(Int8Type)                      \
  PROCESS(UInt16Type)                    \
  PROCESS(Int16Type)                     \
  PROCESS(UInt32Type)                    \
  PROCESS(Int32Type)                     \
  PROCESS(UInt64Type)                    \
  PROCESS(Int64Type)                     \
  PROCESS(FloatType)                     \
  PROCESS(DoubleType)                    \
  PROCESS(Date32Type)                    \
  PROCESS(Date64Type)                    \
  PROCESS(Time32Type)                    \
  PROCESS(Time64Type)                    \
  PROCESS(TimestampType)                 \
  PROCESS(BinaryType)                    \
  PROCESS(StringType)                    \
  PROCESS(FixedSizeBinaryType)           \
  PROCESS(Decimal128Type)

Status Unique(FunctionContext* ctx, const Datum& value, std::shared_ptr<Array>* out) {
  switch (value.type()->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    auto memo_table = std::make_shared<MemoTableType>(ctx->memory_pool());             \
    return Unique<InType>(ctx, value, memo_table, out);                                \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case NullType::type_id: {
      std::shared_ptr<internal::MemoTable> memo_table;
      return Unique<NullType>(ctx, value, memo_table, out);
    } break;
    default:
      break;
  }
  return arrow::Status::OK();
}

Status DictionaryEncode(FunctionContext* ctx, const Datum& value, Datum* out) {
  switch (value.type()->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    auto memo_table = std::make_shared<MemoTableType>(ctx->memory_pool());             \
    return DictionaryEncode<InType>(ctx, value, memo_table, out);                      \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case NullType::type_id: {
      std::shared_ptr<internal::MemoTable> memo_table;
      return DictionaryEncode<NullType>(ctx, value, memo_table, out);
    } break;
    default:
      break;
  }
  return arrow::Status::OK();
}

Status ValueCounts(FunctionContext* ctx, const Datum& value,
                   std::shared_ptr<Array>* counts) {
  switch (value.type()->id()) {
#define PROCESS(InType)                                                                \
  case InType::type_id: {                                                              \
    using MemoTableType = typename arrow::internal::HashTraits<InType>::MemoTableType; \
    auto memo_table = std::make_shared<MemoTableType>(ctx->memory_pool());             \
    return ValueCounts<InType>(ctx, value, memo_table, counts);                        \
  } break;
    PROCESS_SUPPORTED_TYPES(PROCESS)
#undef PROCESS
    case NullType::type_id: {
      std::shared_ptr<internal::MemoTable> memo_table;
      return ValueCounts<NullType>(ctx, value, memo_table, counts);
    } break;
    default:
      break;
  }
  return arrow::Status::OK();
}

#undef PROCESS_SUPPORTED_TYPES
}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/kernels/sort_arrays_to_indices.h"
#include <arrow/util/checked_cast.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <memory>
#include <numeric>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/type_traits.h"

namespace arrow {

class Array;

namespace compute {

/// \brief UnaryKernel implementing SortArraysToIndices operation
class ARROW_EXPORT SortArraysToIndicesKernel {
 protected:
  std::shared_ptr<DataType> type_;

 public:
  /// \brief UnaryKernel interface
  ///
  /// delegates to subclasses via SortArraysToIndices()
  virtual Status Call(FunctionContext* ctx, std::vector<std::shared_ptr<Array>> values,
                      std::shared_ptr<Array>* offsets) = 0;

  /// \brief output type of this kernel
  std::shared_ptr<DataType> out_type() const { return uint64(); }

  /// \brief single-array implementation
  virtual Status SortArraysToIndices(FunctionContext* ctx,
                                     std::vector<std::shared_ptr<Array>> values,
                                     std::shared_ptr<Array>* offsets) = 0;

  /// \brief factory for SortArraysToIndicesKernel
  ///
  /// \param[in] value_type constructed SortArraysToIndicesKernel will support sorting
  ///            values of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<SortArraysToIndicesKernel>* out);
};

template <typename ArrayType>
bool CompareValues(std::vector<std::shared_ptr<ArrayType>> arrays, ArrayItemIndex lhs,
                   ArrayItemIndex rhs) {
  return arrays[lhs.array_id]->Value(lhs.id) < arrays[rhs.array_id]->Value(rhs.id);
}

template <typename ArrayType>
bool CompareViews(std::vector<std::shared_ptr<ArrayType>> arrays, ArrayItemIndex lhs,
                  ArrayItemIndex rhs) {
  return arrays[lhs.array_id]->GetView(lhs.id) < arrays[rhs.array_id]->GetView(rhs.id);
}

template <typename ArrowType, typename Comparator>
class SortArraysToIndicesKernelImpl : public SortArraysToIndicesKernel {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  explicit SortArraysToIndicesKernelImpl(Comparator compare) : compare_(compare) {}

  Status SortArraysToIndices(FunctionContext* ctx,
                             std::vector<std::shared_ptr<Array>> values,
                             std::shared_ptr<Array>* offsets) {
    return SortArraysToIndicesImpl(ctx, values, offsets);
  }

  Status Call(FunctionContext* ctx, std::vector<std::shared_ptr<Array>> values,
              std::shared_ptr<Array>* offsets) override {
    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(this->SortArraysToIndices(ctx, values, &offsets_array));
    *offsets = offsets_array;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return type_; }

 private:
  Comparator compare_;
  std::vector<std::shared_ptr<ArrayType>> typed_arrays_;
  uint64_t merge_time = 0;

  std::pair<ArrayItemIndex*, ArrayItemIndex*> merge(
      std::vector<std::pair<ArrayItemIndex*, ArrayItemIndex*>>::iterator
          arrays_valid_range_begin,
      std::vector<std::pair<ArrayItemIndex*, ArrayItemIndex*>>::iterator
          arrays_valid_range_end) {
    auto size = arrays_valid_range_end - arrays_valid_range_begin;
    // std::cout << "merge of size " << size << std::endl;
    std::pair<ArrayItemIndex*, ArrayItemIndex*> left;
    std::pair<ArrayItemIndex*, ArrayItemIndex*> right;
    if (size > 2) {
      auto half_size = size / 2;
      auto arrays_valid_range_middle = arrays_valid_range_begin + half_size;
      left = merge(arrays_valid_range_begin, arrays_valid_range_middle);
      right = merge(arrays_valid_range_middle, arrays_valid_range_end);
    } else if (size == 2) {
      left = *arrays_valid_range_begin;
      right = *(arrays_valid_range_end - 1);
    } else {
      // only one item
      return *arrays_valid_range_begin;
    }
    auto left_size = left.second - left.first;
    auto right_size = right.second - right.first;

    // std::cout << "left begin is " << left.first << ", end is " << left.second
    //          << std::endl;
    // std::cout << "right begin is " << right.first << ", end is " << right.second
    //          << std::endl;
    // std::cout << "left_size is " << left_size << ", right_size is " << right_size
    //          << std::endl;
    ArrayItemIndex* left_tmp = new ArrayItemIndex[left_size];
    memcpy(left_tmp, left.first, left_size * sizeof(ArrayItemIndex));
    ArrayItemIndex* right_tmp = new ArrayItemIndex[right_size];
    memcpy(right_tmp, right.first, right_size * sizeof(ArrayItemIndex));

    auto start_merge = std::chrono::steady_clock::now();
    std::set_union(left_tmp, left_tmp + left_size, right_tmp, right_tmp + right_size,
                   left.first, [this](ArrayItemIndex left, ArrayItemIndex right) {
                     return typed_arrays_[left.array_id]->GetView(left.id) <
                            typed_arrays_[right.array_id]->GetView(right.id);
                   });
    auto end_merge = std::chrono::steady_clock::now();
    merge_time +=
        std::chrono::duration_cast<std::chrono::microseconds>(end_merge - start_merge)
            .count();
    delete[] left_tmp;
    delete[] right_tmp;

    assert((left.first + left_size + right_size) == right.second);
    // std::cout << "return begin is " << left.first << ", end is " << right.second
    //          << std::endl;
    return std::make_pair(left.first, right.second);
  }

  Status SortArraysToIndicesImpl(FunctionContext* ctx,
                                 std::vector<std::shared_ptr<Array>> values,
                                 std::shared_ptr<Array>* offsets) {
    // initiate buffer for all arrays
    std::shared_ptr<Buffer> indices_buf;
    int64_t items_total = 0;
    for (auto array : values) {
      items_total += array->length();
    }
    int64_t buf_size = items_total * sizeof(ArrayItemIndex);
    RETURN_NOT_OK(AllocateBuffer(ctx->memory_pool(), sizeof(ArrayItemIndex) * buf_size,
                                 &indices_buf));

    // start to partition not_null with null
    ArrayItemIndex* indices_begin =
        reinterpret_cast<ArrayItemIndex*>(indices_buf->mutable_data());
    ArrayItemIndex* indices_end = indices_begin + items_total;
    std::vector<std::pair<ArrayItemIndex*, ArrayItemIndex*>> arrays_valid_range;

    int64_t array_id = 0;
    int64_t null_count_total = 0;
    int64_t indices_i = 0;
    uint64_t array_sort = 0;

    auto start_prepare = std::chrono::steady_clock::now();
    for (auto array : values) {
      auto typed_array = std::dynamic_pointer_cast<ArrayType>(array);
      typed_arrays_.push_back(typed_array);
      auto array_begin = indices_begin + indices_i;
      for (int64_t i = 0; i < array->length(); i++) {
        if (!array->IsNull(i)) {
          (indices_begin + indices_i)->array_id = array_id;
          (indices_begin + indices_i)->id = i;
          indices_i++;
        } else {
          (indices_end - null_count_total - 1)->array_id = array_id;
          (indices_end - null_count_total - 1)->id = i;
          null_count_total++;
        }
      }
      // first round sort
      auto array_end = indices_begin + indices_i;
      auto start_sort = std::chrono::steady_clock::now();
      std::stable_sort(array_begin, array_end,
                       [typed_array, this](ArrayItemIndex left, ArrayItemIndex right) {
                         return typed_array->GetView(left.id) <
                                typed_array->GetView(right.id);
                       });
      auto end_sort = std::chrono::steady_clock::now();
      auto tmp_time =
          std::chrono::duration_cast<std::chrono::microseconds>(end_sort - start_sort)
              .count();
      std::cout << "sort of " << array_end - array_begin << " items took " << tmp_time
                << " us." << std::endl;
      array_sort += tmp_time;
      // std::cout << "begin is " << array_begin << ", end is " << array_end << std::endl;
      arrays_valid_range.push_back(std::make_pair(array_begin, array_end));
      array_id++;
    }
    auto end_prepare = std::chrono::steady_clock::now();

    auto start = std::chrono::steady_clock::now();
    // merge sort
    merge(arrays_valid_range.begin(), arrays_valid_range.end());
    auto end = std::chrono::steady_clock::now();
    auto prepare =
        std::chrono::duration_cast<std::chrono::microseconds>(end_prepare - start_prepare)
            .count();
    auto sort =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::cout << "prepare took " << prepare << " us, includes array sort " << array_sort
              << " us, sort took " << sort << " us, includes merge took " << merge_time
              << " us." << std::endl;

    *offsets = std::make_shared<FixedSizeBinaryArray>(
        std::make_shared<FixedSizeBinaryType>(sizeof(ArrayItemIndex) / sizeof(int32_t)),
        items_total, indices_buf);
    return Status::OK();
  }
};

template <typename ArrowType, typename Comparator>
SortArraysToIndicesKernelImpl<ArrowType, Comparator>* MakeSortArraysToIndicesKernelImpl(
    Comparator comparator) {
  return new SortArraysToIndicesKernelImpl<ArrowType, Comparator>(comparator);
}

Status SortArraysToIndicesKernel::Make(const std::shared_ptr<DataType>& value_type,
                                       std::unique_ptr<SortArraysToIndicesKernel>* out) {
  SortArraysToIndicesKernel* kernel;
  switch (value_type->id()) {
    case Type::UINT8:
      kernel = MakeSortArraysToIndicesKernelImpl<UInt8Type>(CompareValues<UInt8Array>);
      break;
    case Type::INT8:
      kernel = MakeSortArraysToIndicesKernelImpl<Int8Type>(CompareValues<Int8Array>);
      break;
    case Type::UINT16:
      kernel = MakeSortArraysToIndicesKernelImpl<UInt16Type>(CompareValues<UInt16Array>);
      break;
    case Type::INT16:
      kernel = MakeSortArraysToIndicesKernelImpl<Int16Type>(CompareValues<Int16Array>);
      break;
    case Type::UINT32:
      kernel = MakeSortArraysToIndicesKernelImpl<UInt32Type>(CompareValues<UInt32Array>);
      break;
    case Type::INT32:
      kernel = MakeSortArraysToIndicesKernelImpl<Int32Type>(CompareValues<Int32Array>);
      break;
    case Type::UINT64:
      kernel = MakeSortArraysToIndicesKernelImpl<UInt64Type>(CompareValues<UInt64Array>);
      break;
    case Type::INT64:
      kernel = MakeSortArraysToIndicesKernelImpl<Int64Type>(CompareValues<Int64Array>);
      break;
    case Type::FLOAT:
      kernel = MakeSortArraysToIndicesKernelImpl<FloatType>(CompareValues<FloatArray>);
      break;
    case Type::DOUBLE:
      kernel = MakeSortArraysToIndicesKernelImpl<DoubleType>(CompareValues<DoubleArray>);
      break;
    case Type::BINARY:
      kernel = MakeSortArraysToIndicesKernelImpl<BinaryType>(CompareViews<BinaryArray>);
      break;
    case Type::STRING:
      kernel = MakeSortArraysToIndicesKernelImpl<StringType>(CompareViews<StringArray>);
      break;
    default:
      return Status::NotImplemented("Sorting of ", *value_type, " arrays");
  }
  out->reset(kernel);
  return Status::OK();
}

Status SortArraysToIndices(FunctionContext* ctx,
                           std::vector<std::shared_ptr<Array>> values,
                           std::shared_ptr<Array>* offsets) {
  if (values.size() == 0) {
    return Status::Invalid("Input ArrayList is empty");
  }
  std::unique_ptr<SortArraysToIndicesKernel> kernel;
  RETURN_NOT_OK(SortArraysToIndicesKernel::Make(values[0]->type(), &kernel));
  return kernel->Call(ctx, values, offsets);
}

}  // namespace compute
}  // namespace arrow

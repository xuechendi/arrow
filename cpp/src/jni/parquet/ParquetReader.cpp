#include "ParquetReader.h"
#include <iostream>
#include "stdlib.h"
#include <arrow/io/hdfs.h>
#include <arrow/record_batch.h>

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

ParquetReader::ParquetReader(
    HdfsConnector* hdfsReader,
    std::vector<int>& column_indices,
    std::vector<int>& row_group_indices,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false),
  hdfsReader(hdfsReader) {
  hdfsReader->openAndSeek(&file, 0);
  properties.set_batch_size(batch_size);

  Status msg = ::parquet::arrow::FileReader::Make(
      pool,
      ::parquet::ParquetFileReader::Open(file),
      properties,
      &arrow_reader);
  if (!msg.ok()) {
    std::cerr << "Open hdfs parquet file failed, error msg: " << msg << std::endl;
    exit(-1);
  }

  msg = getRecordBatch(row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatch failed, error msg: " << msg << std::endl;
    exit(-1);
  }
}

ParquetReader::ParquetReader(
    HdfsConnector* hdfsReader,
    std::vector<int>& column_indices,
    long start_pos,
    long end_pos,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false),
  hdfsReader(hdfsReader){
  hdfsReader->openAndSeek(&file, 0);
  properties.set_batch_size(batch_size);

  Status msg = ::parquet::arrow::FileReader::Make(
      pool,
      ::parquet::ParquetFileReader::Open(file),
      properties,
      &arrow_reader);
  if (!msg.ok()) {
    std::cerr << "Open hdfs parquet file failed, error msg: " << msg << std::endl;
    exit(-1);
  }

  std::vector<int> row_group_indices =
    getRowGroupIndices(arrow_reader->num_row_groups(), start_pos, end_pos);
  msg = getRecordBatch(row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatch failed, error msg: " << msg << std::endl;
    exit(-1);
  }
}

ParquetReader::~ParquetReader(){
  //std::cerr << "~ParquetReader" << std::endl;
}

std::vector<int> ParquetReader::getRowGroupIndices(
    int num_row_groups, long start_pos, long end_pos) {
  std::unique_ptr<::parquet::ParquetFileReader> reader = ::parquet::ParquetFileReader::Open(file);
  std::vector<int> row_group_indices;
  long pos = 0;
  for (int i = 0; i < num_row_groups; i++) {
    if (pos >= start_pos && pos < end_pos) {
      row_group_indices.push_back(i);
      std::cerr << "pos: " << pos << " ,range: " << start_pos << "-" << end_pos << ", find row group index: " << i << std::endl;
      break;
    }
    pos += reader->RowGroup(i)->metadata()->total_byte_size();
  }
  if (row_group_indices.empty()) {
    row_group_indices.push_back(num_row_groups - 1);
  }
  return row_group_indices;
} 

Status ParquetReader::getRecordBatch(
    std::vector<int>& row_group_indices,
    std::vector<int>& column_indices) {
  if (column_indices.empty()) {
    return arrow_reader->GetRecordBatchReader(row_group_indices, &rb_reader);
  } else {
    return arrow_reader->GetRecordBatchReader(row_group_indices, column_indices, &rb_reader);
  }
}

Status ParquetReader::readNext(std::shared_ptr<::arrow::RecordBatch>* out){
  std::lock_guard<std::mutex> lck (threadMtx);
  return rb_reader->ReadNext(out);
}

void ParquetReader::print(std::shared_ptr<::arrow::RecordBatch> out) {
  std::cout << "this record batch has numColumns " << out->num_columns() << ", and numRows is "
    << out->num_rows() << std::endl;
  for (int i = 0; i < 10; i++) {
    auto array = out->column(i);
    std::cout << out->column_name(i) << std::endl;
  }
  std::cout << std::endl;
}


std::shared_ptr<Schema> ParquetReader::schema() {
  return rb_reader->schema();
}

}//parquet
}//jni

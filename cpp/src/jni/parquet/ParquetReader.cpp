#include <iostream>
#include "stdlib.h"
#include <arrow/record_batch.h>
#include "ParquetReader.h"
#include "HdfsConnector.h"
#include "FileConnector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

ParquetReader::ParquetReader(
    std::string path,
    std::vector<int>& column_indices,
    std::vector<int>& row_group_indices,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false) {
  if (path.find("hdfs") != std::string::npos) {
    connector = new HdfsConnector(path);
  } else {
    connector = new FileConnector(path);
  }
  connector->openReadable();
  properties.set_batch_size(batch_size);

  Status msg = ::parquet::arrow::FileReader::Make(
      pool,
      ::parquet::ParquetFileReader::Open(connector->getReader()),
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
    std::string path,
    std::vector<int>& column_indices,
    long start_pos,
    long end_pos,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false) {
  if (path.find("hdfs") != std::string::npos) {
    connector = new HdfsConnector(path);
  } else {
    connector = new FileConnector(path);
  }
  connector->openReadable();
  properties.set_batch_size(batch_size);

  Status msg = ::parquet::arrow::FileReader::Make(
      pool,
      ::parquet::ParquetFileReader::Open(connector->getReader()),
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
  connector->teardown();
  delete connector;
}

std::vector<int> ParquetReader::getRowGroupIndices(
    int num_row_groups, long start_pos, long end_pos) {
  std::unique_ptr<::parquet::ParquetFileReader> reader =
    ::parquet::ParquetFileReader::Open(connector->getReader());
  std::vector<int> row_group_indices;
  long pos = 0;
  for (int i = 0; i < num_row_groups; i++) {
    if (pos >= start_pos && pos < end_pos) {
      row_group_indices.push_back(i);
      //std::cerr << "pos: " << pos << " ,range: " << start_pos << "-" << end_pos << ", find row group index: " << i << std::endl;
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


std::shared_ptr<Schema> ParquetReader::schema() {
  return rb_reader->schema();
}

}//parquet
}//jni

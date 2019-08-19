#include "ParquetHdfsRecordBatchReader.h"
#include <iostream>
#include "stdlib.h"
#include <arrow/io/hdfs.h>
#include <arrow/record_batch.h>

namespace arrow {
namespace io {

ParquetHdfsRecordBatchReader::ParquetHdfsRecordBatchReader(
    ParquetHdfsReader* hdfsReader,
    std::vector<int>& column_indices,
    std::vector<int>& row_group_indices,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false),
  hdfsReader(hdfsReader) {
  hdfsReader->openAndSeek(&file, 0);
  properties.set_batch_size(batch_size);

  arrow_reader.reset(
      new ::parquet::arrow::FileReader(
        pool,
        ::parquet::ParquetFileReader::Open(file),
        properties));

  Status msg = getRecordBatch(&rb_reader, row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatch failed, error msg: " << msg << std::endl;
    exit(-1);
  }
}

ParquetHdfsRecordBatchReader::ParquetHdfsRecordBatchReader(
    ParquetHdfsReader* hdfsReader,
    std::vector<int>& column_indices,
    long start_pos,
    long end_pos,
    long batch_size):
  pool(::arrow::default_memory_pool()),
  properties(false),
  hdfsReader(hdfsReader){
  hdfsReader->openAndSeek(&file, 0);
  properties.set_batch_size(batch_size);

  arrow_reader.reset(
      new ::parquet::arrow::FileReader(
        pool,
        ::parquet::ParquetFileReader::Open(file),
        properties));

  std::vector<int> row_group_indices =
    getRowGroupIndices(arrow_reader->num_row_groups(), start_pos, end_pos);
  Status msg = getRecordBatch(&rb_reader, row_group_indices, column_indices);
  if (!msg.ok()) {
    std::cerr << "GetRecordBatch failed, error msg: " << msg << std::endl;
    exit(-1);
  }
}

ParquetHdfsRecordBatchReader::~ParquetHdfsRecordBatchReader(){
  //std::cerr << "~ParquetHdfsRecordBatchReader" << std::endl;
}

std::vector<int> ParquetHdfsRecordBatchReader::getRowGroupIndices(
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
  return row_group_indices;
} 

Status ParquetHdfsRecordBatchReader::getRecordBatch(
    std::shared_ptr<::arrow::RecordBatchReader>* rb_reader,
    std::vector<int>& row_group_indices,
    std::vector<int>& column_indices) {
  //std::cerr << "row_group_indices has element number " << row_group_indices.size() << std::endl;
  //std::cerr << "column_indices has element number " << column_indices.size() << std::endl;
  if (column_indices.empty()) {
    return arrow_reader->GetRecordBatchReader(row_group_indices, rb_reader);
  } else {
    return arrow_reader->GetRecordBatchReader(row_group_indices, column_indices, rb_reader);
  }
}

Status ParquetHdfsRecordBatchReader::readNext(std::shared_ptr<::arrow::RecordBatch>* out){
  std::lock_guard<std::mutex> lck (threadMtx);
  return rb_reader->ReadNext(out);
}

void ParquetHdfsRecordBatchReader::print(std::shared_ptr<::arrow::RecordBatch> out) {
  std::cout << "this record batch has numColumns " << out->num_columns() << ", and numRows is "
    << out->num_rows() << std::endl;
  for (int i = 0; i < 10; i++) {
    auto array = out->column(i);
    std::cout << out->column_name(i) << std::endl;
  }
  std::cout << std::endl;
}


std::shared_ptr<Schema> ParquetHdfsRecordBatchReader::schema() {
  return rb_reader->schema();
}

}//io
}//arrow

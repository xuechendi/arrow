#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include "HdfsConnector.h"
#include <arrow/record_batch.h>
#include <parquet/properties.h>
#include <parquet/file_reader.h>
#include <parquet/arrow/reader.h>
#include <mutex>

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class ParquetReader {
public:
  ParquetReader(
    HdfsConnector* hdfsReader,
    std::vector<int>& column_indices,
    std::vector<int>& row_group_indices,
    long batch_size);
  ParquetReader(
    HdfsConnector* hdfsReader,
    std::vector<int>& column_indices,
    long start_pos,
    long end_pos,
    long batch_size);
  ~ParquetReader();
  Status readNext(std::shared_ptr<::arrow::RecordBatch>* out);
  std::shared_ptr<Schema> schema();
  HdfsConnector* hdfsConnector;

private:
  MemoryPool* pool;
  std::mutex threadMtx;

  std::unique_ptr<::parquet::arrow::FileReader> arrow_reader;
  std::shared_ptr<HdfsReadableFile> file;
  ::parquet::ArrowReaderProperties properties;
  std::shared_ptr<RecordBatchReader> rb_reader;

  std::vector<int> getRowGroupIndices(int num_row_groups, long start_pos, long end_pos);
  Status getRecordBatch(std::vector<int>& row_group_indices, std::vector<int>& column_indices);
};
}
}

#endif

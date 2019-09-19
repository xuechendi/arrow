#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include "HdfsConnector.h"
#include <arrow/table.h>
#include <parquet/properties.h>
#include <parquet/file_writer.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/schema.h>
#include <mutex>

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class ParquetWriter {
public:
  ParquetWriter(
      HdfsConnector* hdfsConnector,
      std::shared_ptr<Schema>& schema);
  ~ParquetWriter();
  Status writeNext(int num_rows, long* in_buf_addrs, long* in_buf_sizes, int in_bufs_len);
  Status writeNext(std::shared_ptr<RecordBatch>& rb);
  Status flush();

private:
  MemoryPool* pool;
  std::mutex threadMtx;
  std::unique_ptr<::parquet::arrow::FileWriter> arrow_writer;
  std::shared_ptr<HdfsOutputStream> file;
  HdfsConnector* hdfsConnector;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<::parquet::SchemaDescriptor> schema_description;
  std::vector<std::shared_ptr<RecordBatch>> record_batch_buffer_list;

  Status makeRecordBatch(
      std::shared_ptr<Schema> &schema,
      int num_rows,
      long* in_buf_addrs,
      long* in_buf_sizes,
      int in_bufs_len,
      std::shared_ptr<arrow::RecordBatch> *batch);
};
}
}

#endif

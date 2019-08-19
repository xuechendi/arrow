#include "ParquetWriter.h"
#include <iostream>
#include <stdlib.h>
#include "HdfsConnector.h"
#include "FileConnector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

ParquetWriter::ParquetWriter(
    std::string path,
    std::shared_ptr<Schema>& schema):
  pool(default_memory_pool()),
  schema(schema) {
  if (path.find("hdfs") != std::string::npos) {
    connector = new HdfsConnector(path);
  } else {
    connector = new FileConnector(path);
  }
  connector->openWritable();
  std::shared_ptr<::parquet::WriterProperties> properties = ::parquet::default_writer_properties();
  Status msg = ::parquet::arrow::ToParquetSchema(schema.get(), *properties.get(), &schema_description);
  if (!msg.ok()) {
    std::cerr << "Convert Arrow Schema to Parquet Schema failed, error msg: " << msg << std::endl;
    exit(-1);
  }

  ::parquet::schema::NodeVector group_node_fields;
  for (int i = 0; i < schema_description->group_node()->field_count(); i++) {
    group_node_fields.push_back(schema_description->group_node()->field(i));
  } 
  auto parquet_schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
      ::parquet::schema::GroupNode::Make(
        schema_description->schema_root()->name(),
        schema_description->schema_root()->repetition(),
        group_node_fields));

  msg = ::parquet::arrow::FileWriter::Make(
      pool,
      ::parquet::ParquetFileWriter::Open(connector->getWriter(), parquet_schema),
      schema,
      ::parquet::default_arrow_writer_properties(),
      &arrow_writer);
  if (!msg.ok()) {
    std::cerr << "Open parquet file failed, error msg: " << msg << std::endl;
    exit(-1);
  }
}

ParquetWriter::~ParquetWriter(){
  Status msg = arrow_writer->Close();
  if (!msg.ok()) {
    std::cerr << "ParquetFileWriter close failed, error msg: " << msg << std::endl;
    exit(-1);
  }
  connector->teardown();
  delete connector;
}

Status ParquetWriter::writeNext(int num_rows, long* in_buf_addrs, long* in_buf_sizes, int in_bufs_len) {
  std::shared_ptr<RecordBatch> batch;
  Status msg = makeRecordBatch(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &batch);
  if (!msg.ok()) {
    return msg;
  }

  std::lock_guard<std::mutex> lck (threadMtx);
  record_batch_buffer_list.push_back(batch);

  return msg;
}

Status ParquetWriter::flush() {
  std::shared_ptr<Table> table;
  Status msg = Table::FromRecordBatches(record_batch_buffer_list, &table);
  if (!msg.ok()) {
    std::cerr << "Table::FromRecordBatches failed" << std::endl;
    return msg;
  }

  msg = arrow_writer->WriteTable(*table.get(), table->num_rows());
  if (!msg.ok()) {
    std::cerr << "arrow_writer->WriteTable failed" << std::endl;
    return msg;
  }

  msg = connector->getWriter()->Flush();
  return msg;
}

Status ParquetWriter::writeNext(std::shared_ptr<RecordBatch>& rb) {
  std::lock_guard<std::mutex> lck (threadMtx);
  record_batch_buffer_list.push_back(rb);
  return Status::OK();
}

Status ParquetWriter::makeRecordBatch(std::shared_ptr<Schema> &schema, int num_rows, long* in_buf_addrs, long* in_buf_sizes, int in_bufs_len, std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<ArrayData>> arrays;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;
  int sz_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::vector<std::shared_ptr<Buffer>> buffers;

    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    long validity_addr = in_buf_addrs[buf_idx++];
    long validity_size = in_buf_sizes[sz_idx++];
    auto validity = std::shared_ptr<Buffer>(
      new Buffer(reinterpret_cast<uint8_t*>(validity_addr), validity_size));
    buffers.push_back(validity);

    if (buf_idx >= in_bufs_len) {
      return Status::Invalid("insufficient number of in_buf_addrs");
    }
    long value_addr = in_buf_addrs[buf_idx++];
    long value_size = in_buf_sizes[sz_idx++];
    auto data = std::shared_ptr<Buffer>(
        new Buffer(reinterpret_cast<uint8_t*>(value_addr), value_size));
    buffers.push_back(data);

    if (arrow::is_binary_like(field->type()->id())) {
      if (buf_idx >= in_bufs_len) {
        return Status::Invalid("insufficient number of in_buf_addrs");
      }

      // add offsets buffer for variable-len fields.
      long offsets_addr = in_buf_addrs[buf_idx++];
      long offsets_size = in_buf_sizes[sz_idx++];
      auto offsets = std::shared_ptr<Buffer>(
          new Buffer(reinterpret_cast<uint8_t*>(offsets_addr), offsets_size));
      buffers.push_back(offsets);
    }

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, std::move(buffers));
    arrays.push_back(array_data);
  }
  *batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
  return Status::OK();
}

}//parquet
}//jni

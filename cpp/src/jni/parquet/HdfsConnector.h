#ifndef HDFS_CONNECTOR_H
#define HDFS_CONNECTOR_H

#include <arrow/buffer.h>
#include <arrow/io/hdfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <arrow/array.h>
#include <cstring>

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class HdfsConnector {
public:
  HdfsConnector(std::string hdfsPath);
  ~HdfsConnector();
  Status setupHdfsClient();
  void getHdfsHostAndPort(std::string hdfsPath, HdfsConnectionConfig *hdfs_conf);
  std::string getFileName();
  std::string getFileName(std::string filePath);
  Status openReadable(std::shared_ptr<HdfsReadableFile>* file);
  Status openWritable(std::shared_ptr<HdfsOutputStream>* file, int32_t buffer_size = 0, int16_t replication = 1, int64_t default_block_size = 0);
  void teardown();

protected:
  std::shared_ptr<HadoopFileSystem> hdfsClient;
  HdfsConnectionConfig hdfsConfig;
  bool driverLoaded;
  std::string hdfsFilePath;
  Status mkdir(std::string path);
  std::string getPathDir(std::string path);
};
}//parquet
}//jni

#endif

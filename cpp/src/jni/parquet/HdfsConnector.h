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
  Status openAndSeek(std::shared_ptr<HdfsReadableFile>* file, int64_t pos);
  void teardown();

protected:
  std::shared_ptr<HadoopFileSystem> hdfsClient;
  HdfsConnectionConfig hdfsConfig;
  bool driverLoaded;
  std::string hdfsFilePath;
};
}//parquet
}//jni

#endif

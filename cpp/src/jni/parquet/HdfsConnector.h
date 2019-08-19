#ifndef HDFS_CONNECTOR_H
#define HDFS_CONNECTOR_H

#include <arrow/io/hdfs.h>
#include "Connector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class HdfsConnector : public Connector {
public:
  HdfsConnector(std::string hdfsPath);
  ~HdfsConnector();

  Status openReadable();
  Status openWritable();
  std::shared_ptr<RandomAccessFile> getReader() {
    return fileReader;
  }
  std::shared_ptr<OutputStream> getWriter() {
    return fileWriter;
  }
  void teardown();

protected:
  std::shared_ptr<HadoopFileSystem> hdfsClient;
  std::shared_ptr<HdfsReadableFile> fileReader;
  std::shared_ptr<HdfsOutputStream> fileWriter;
  HdfsConnectionConfig hdfsConfig;
  bool driverLoaded = false;
  int32_t buffer_size = 0;
  int16_t replication = 1;
  int64_t default_block_size = 0;

  Status setupHdfsClient();
  Status getHdfsHostAndPort(std::string hdfsPath, HdfsConnectionConfig *hdfs_conf);
  std::string getFileName(std::string filePath);
  Status mkdir(std::string path);
};
}//parquet
}//jni

#endif

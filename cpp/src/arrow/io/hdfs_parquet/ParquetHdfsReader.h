#ifndef PARQUET_HDFS_READER_H
#define PARQUET_HDFS_READER_H

#include <arrow/buffer.h>
#include <arrow/io/hdfs.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <arrow/array.h>
#include <cstring>
#include <parquet/file_reader.h>
#include <parquet/arrow/reader.h>

namespace arrow {
namespace io {

class ParquetHdfsReader {
public:
  ParquetHdfsReader(std::string hdfsPath);
  ~ParquetHdfsReader();
  Status setupHdfsClient();
  void getHdfsHostAndPort(std::string hdfsPath, HdfsConnectionConfig *hdfs_conf);
  std::string getFileName();
  std::string getFileName(std::string filePath);
  Status openAndSeek(std::shared_ptr<HdfsReadableFile>* file, int64_t pos);
  bool ifHitEndSplit(std::shared_ptr<HdfsReadableFile> file, int64_t end_pos);
  void teardown();

protected:
  std::shared_ptr<HadoopFileSystem> hdfsClient;
  HdfsConnectionConfig hdfsConfig;
  bool driverLoaded;
  std::string hdfsFilePath;
};
}//io
}//arrow

#endif

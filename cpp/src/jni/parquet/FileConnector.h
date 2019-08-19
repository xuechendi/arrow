#ifndef FILE_CONNECTOR_H
#define FILE_CONNECTOR_H

#include <arrow/io/file.h>
#include "Connector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class FileConnector : public Connector {
public:
  FileConnector(std::string path);
  ~FileConnector();
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
  Status mkdir(std::string path);
  std::shared_ptr<ReadableFile> fileReader;
  std::shared_ptr<OutputStream> fileWriter;
};
}//parquet
}//jni

#endif

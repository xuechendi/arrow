#ifndef CONNECTOR_H
#define CONNECTOR_H

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <arrow/array.h>
#include <cstring>
#include <iostream>

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

class Connector {
public:
  Connector() {}
  std::string getFileName() {
    return filePath;
  }
  virtual Status openReadable() = 0;
  virtual Status openWritable() = 0;
  virtual std::shared_ptr<RandomAccessFile> getReader() = 0;
  virtual std::shared_ptr<OutputStream> getWriter() = 0;
  virtual void teardown() = 0;

protected:
  std::string filePath;
  std::string dirPath;
  std::string getPathDir(std::string path) {
    std::string delimiter = "/";
    size_t pos = 0;
    size_t last_pos = pos;
    std::string token;
    while ((pos = path.find(delimiter, pos)) != std::string::npos) {
      last_pos = pos;
      pos += 1;
    }
    if (last_pos == 0) {
      return std::string();
    }
    return path.substr(0, last_pos + delimiter.length());
  }
  virtual Status mkdir(std::string path) = 0;
};
}//parquet
}//jni

#endif

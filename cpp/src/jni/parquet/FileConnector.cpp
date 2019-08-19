#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <vector>
#include <numeric>
#include <cstdlib>

#include "FileConnector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

FileConnector::FileConnector(std::string path) {
  filePath = path;
  dirPath = getPathDir(path);
}

FileConnector::~FileConnector() {
}

void FileConnector::teardown() {
  if (fileWriter) {
    fileWriter->Close();
  }
}

Status FileConnector::openReadable() {
  Status msg = ReadableFile::Open(filePath, &fileReader);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is "
      << filePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status FileConnector::openWritable() {
  Status msg;
  if (!dirPath.empty()) {
    msg = mkdir(dirPath);
    if (!msg.ok()) {
      std::cerr << "mkdir for path failed "
        << dirPath << ", error is : " << msg << std::endl;
      exit(-1);
    }
  }

  msg = FileOutputStream::Open(filePath, false, &fileWriter);
  if (!msg.ok()) {
    std::cerr << "Open file failed, file name is "
      << filePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status FileConnector::mkdir(std::string path) {
  std::string cmd = "mkdir -p ";
  cmd.append(path);
  const int ret = system(cmd.c_str());
  //int ret = ::mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  if (ret < 0 && ret != EEXIST) {
    return Status::IOError(strerror(ret));
  }
  return Status::OK();
}

}//parquet
}//jni

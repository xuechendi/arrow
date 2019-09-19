#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <vector>
#include <numeric>
#include<stdlib.h>

#include "HdfsConnector.h"

namespace jni {
namespace parquet {

using namespace ::arrow;
using namespace ::arrow::io;

HdfsConnector::HdfsConnector(std::string hdfsPath):
  driverLoaded(false),
  hdfsClient(nullptr) {
  getHdfsHostAndPort(hdfsPath, &hdfsConfig);
  hdfsFilePath = getFileName(hdfsPath);
  Status msg;

  msg = setupHdfsClient();
  if (!msg.ok()) {
    std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
    exit(-1);
  }
}

HdfsConnector::~HdfsConnector() {
  teardown();
}

Status HdfsConnector::setupHdfsClient() {
  bool useHdfs3 = true;
  if (hdfsClient != nullptr) {
    return Status::OK();
  }
  if (!driverLoaded) {
    Status msg;
    if (useHdfs3) {
      hdfsConfig.driver = arrow::io::HdfsDriver::LIBHDFS3;
    } else {
      hdfsConfig.driver = arrow::io::HdfsDriver::LIBHDFS;
    }
    driverLoaded = true;
  }

  return HadoopFileSystem::Connect(&hdfsConfig, &hdfsClient);
}

void HdfsConnector::getHdfsHostAndPort(std::string hdfs_path, HdfsConnectionConfig *hdfs_conf) {
  std::string search_str0 = std::string(":");
  /* hdfs path start with hdfs:// */
  std::string::size_type pos0 = hdfs_path.find_first_of(search_str0, 7);

  std::string search_str1 = std::string("/");
  std::string::size_type pos1 = hdfs_path.find_first_of(search_str1, pos0);
  
  if ((pos0 == std::string::npos) || (pos1 == std::string::npos)) {
      std::cerr << "No host and port information. Use default hdfs port!";
      hdfs_conf->host = "localhost";
      hdfs_conf->port = 20500;
  } else {    
      hdfs_conf->host = hdfs_path.substr(7, pos0 - 7);
      hdfs_conf->port = std::stoul(hdfs_path.substr(pos0 + 1, pos1 - pos0 - 1));    
  }     
}

std::string HdfsConnector::getFileName(std::string filePath) {
  /* Get the path with out hdfs:// prefix. */        
  std::string search_str0 = std::string(":");
  std::string::size_type pos0 = filePath.find_first_of(search_str0, 7);
  std::string hdfsFilePath;
  if (pos0 == std::string::npos) {
      hdfsFilePath = filePath.substr(7, std::string::npos);
  } else {        
      std::string search_str1 = std::string("/");
      std::string::size_type pos1 = filePath.find_first_of(search_str1, 7);
      hdfsFilePath = filePath.substr(pos1, std::string::npos);
  }
  return hdfsFilePath;
}

std::string HdfsConnector::getFileName() {
  return hdfsFilePath;
}

void HdfsConnector::teardown() {
  if (hdfsClient) {
    hdfsClient->Disconnect();
    hdfsClient = nullptr;
  }
}

Status HdfsConnector::openReadable(std::shared_ptr<HdfsReadableFile>* file) {
  Status msg = hdfsClient->OpenReadable(hdfsFilePath, file);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is "
      << hdfsFilePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status HdfsConnector::openWritable(std::shared_ptr<HdfsOutputStream>* file, int32_t buffer_size, int16_t replication, int64_t default_block_size) {
  
  std::string dir = getPathDir(hdfsFilePath);
  Status msg = mkdir(dir);
  if (!msg.ok()) {
    std::cerr << "Mkdir for HDFS path failed "
      << dir << ", error is : " << msg << std::endl;
    exit(-1);
  }

  msg = hdfsClient->OpenWritable(hdfsFilePath, false, buffer_size, replication, default_block_size, file);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is "
      << hdfsFilePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status HdfsConnector::mkdir(std::string path) {

  return hdfsClient->MakeDirectory(path);
}

std::string HdfsConnector::getPathDir(std::string path) {
  std::string delimiter = "/";

  size_t pos = 0;
  size_t last_pos = pos;
  size_t npos = path.length() - 1;
  std::string token;
  while ((pos = path.find(delimiter, pos)) < npos) {
    last_pos = pos;
    pos += 1;
  }
  return path.substr(0, last_pos + delimiter.length());
}


}
}

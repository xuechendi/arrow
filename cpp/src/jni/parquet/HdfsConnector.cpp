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
  driverLoaded(false) {
  getHdfsHostAndPort(hdfsPath, &hdfsConfig);
  filePath = getFileName(hdfsPath);
  dirPath = getPathDir(filePath);
  Status msg;

  msg = setupHdfsClient();
  if (!msg.ok()) {
    std::cerr << "connect HDFS failed, error is : " << msg << std::endl;
    exit(-1);
  }
}

HdfsConnector::~HdfsConnector() {
}

Status HdfsConnector::setupHdfsClient() {
  bool useHdfs3 = true;
  if (hdfsClient) {
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

Status HdfsConnector::getHdfsHostAndPort(std::string hdfs_path, HdfsConnectionConfig *hdfs_conf) {
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
  return Status::OK();
}

std::string HdfsConnector::getFileName(std::string path) {
  /* Get the path with out hdfs:// prefix. */        
  std::string search_str0 = std::string(":");
  std::string::size_type pos0 = path.find_first_of(search_str0, 7);
  std::string fileName;
  if (pos0 == std::string::npos) {
      fileName = path.substr(7, std::string::npos);
  } else {        
      std::string search_str1 = std::string("/");
      std::string::size_type pos1 = path.find_first_of(search_str1, 7);
      fileName = path.substr(pos1, std::string::npos);
  }
  return fileName;
}

void HdfsConnector::teardown() {
  if (fileWriter) {
    fileWriter->Close();
  }
  if (fileReader) {
    fileReader->Close();
  }
  if (hdfsClient) {
    hdfsClient->Disconnect();
    hdfsClient = nullptr;
  }
}

Status HdfsConnector::openReadable() {
  Status msg = hdfsClient->OpenReadable(filePath, &fileReader);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is "
      << filePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status HdfsConnector::openWritable() {
  Status msg;
  if (!dirPath.empty()) {
    msg = mkdir(dirPath);
    if (!msg.ok()) {
      std::cerr << "Mkdir for HDFS path failed "
        << dirPath << ", error is : " << msg << std::endl;
      exit(-1);
    }
  }

  msg = hdfsClient->OpenWritable(
      filePath, false, buffer_size, replication, default_block_size, &fileWriter);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is "
      << filePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  return msg;
}

Status HdfsConnector::mkdir(std::string path) {
  return hdfsClient->MakeDirectory(path);
}


}
}

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

#include "ParquetHdfsReader.h"

namespace arrow {
namespace io {

ParquetHdfsReader::ParquetHdfsReader(std::string hdfsPath):
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
  //std::cerr << "hdfs client connected" << std::endl;
}

ParquetHdfsReader::~ParquetHdfsReader() {
  teardown();
}

Status ParquetHdfsReader::setupHdfsClient() {
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

void ParquetHdfsReader::getHdfsHostAndPort(std::string hdfs_path, HdfsConnectionConfig *hdfs_conf) {
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
  //std::cerr << "host: " << hdfs_conf->host << ", port: " << hdfs_conf->port << std::endl;
}

std::string ParquetHdfsReader::getFileName(std::string filePath) {
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
  //std::cerr << "filePath: " << hdfsFilePath << std::endl;
  return hdfsFilePath;
}

std::string ParquetHdfsReader::getFileName() {
  return hdfsFilePath;
}

void ParquetHdfsReader::teardown() {
  //std::cerr << "Disconnect to hdfs" << std::endl;
  if (hdfsClient) {
    hdfsClient->Disconnect();
    hdfsClient = nullptr;
  }
}

Status ParquetHdfsReader::openAndSeek(std::shared_ptr<HdfsReadableFile>* file, int64_t pos) {
  Status msg = hdfsClient->OpenReadable(hdfsFilePath, file);
  if (!msg.ok()) {
    std::cerr << "Open HDFS file failed, file name is "
      << hdfsFilePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  /*msg = (*file)->Seek(pos);
  if (!msg.ok()) {
    std::cerr << "Seek HDFS file to " << pos << "failed, file name is "
      << hdfsFilePath << ", error is : " << msg << std::endl;
    exit(-1);
  }*/
  return msg;
}

bool ParquetHdfsReader::ifHitEndSplit(std::shared_ptr<HdfsReadableFile> file, int64_t end_pos) {
  int64_t cur_pos;
  Status msg = file->Tell(&cur_pos);
  if (!msg.ok()) {
    std::cerr << "Seek HDFS tell cur position failed, file name is "
      << hdfsFilePath << ", error is : " << msg << std::endl;
    exit(-1);
  }
  std::cerr << hdfsFilePath << " check ifHitEndSplit, cur_pos is " << cur_pos << ", end_pos is " << end_pos << std::endl;
  if (cur_pos >= end_pos) {
    return true;
  } else {
    return false;
  }
}

}
}

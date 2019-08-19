#include <cstring>
#include <iostream>
#include <iomanip>
#include "ParquetHdfsRecordBatchReader.h"

int main(int argc, char *argv[]) {
  std::string path;
  if (argc != 2) {
    path =  "hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2452642/part-00196-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet";
  } else {
    path = argv[1];
  }

  arrow::io::ParquetHdfsRecordBatchReader reader(path);
  std::shared_ptr<::arrow::RecordBatch> out;
  reader.readNext(&out);
  reader.print(out);

}

#include <cstring>
#include <iostream>
#include <iomanip>
#include "ParquetReader.h"
#include "ParquetWriter.h"
#include <arrow/pretty_print.h>

int main(int argc, char *argv[]) {
  std::string path;
  if (argc != 2) {
    path = "hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2452641/part-00094-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet";
    //path = "hdfs://sr602:9000/tpcds_output_test/web_sales/ws_sold_date_sk=2452641/part-00094-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet";
  } else {
    path = argv[1];
  }

  std::cout << "read path is " << path << std::endl;
  std::vector<int> column_indices = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32};
  std::vector<int> row_group_indices = {0};
  jni::parquet::HdfsConnector reader_handler(path);
  jni::parquet::ParquetReader reader(&reader_handler, column_indices, row_group_indices, 4096);
  auto schema = reader.schema();
  std::cout << schema->ToString() << std::endl;

  /*std::string w_path = "hdfs://sr602:9000/tpcds_output_test/web_sales/ws_sold_date_sk=2452641/part-00094-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet";
  std::cout << "w_path is " << w_path << std::endl;
  jni::parquet::HdfsConnector writer_handler(w_path);
  jni::parquet::ParquetWriter writer(&writer_handler, schema);
  */
  int total_batches = 0;
  int total_rows = 0;
  arrow::Status status;
  std::shared_ptr<::arrow::RecordBatch> out;
  int repeat = 0;
  std::ostream stream(nullptr);
  stream.rdbuf(std::cout.rdbuf());
  while(true && repeat++ < 1) {
    status = reader.readNext(&out);
    if (status.ok() && out) {
      total_batches += 1;
      total_rows += out->num_rows();

      PrettyPrint (*out.get(), 20, &stream);
      //writer.writeNext(out);
    } else {
      break;
    }
  }

  std::cout << "Total read batches " << total_batches << ", total rows " << total_rows << std::endl;
}

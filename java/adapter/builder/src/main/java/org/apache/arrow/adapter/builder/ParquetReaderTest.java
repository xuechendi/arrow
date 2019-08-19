/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.builder;

import java.lang.*;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class ParquetReaderTest {

  public static void testParquetReader() throws Exception {
    //String path = new String("hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2451836/part-00092-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet");
    String path = new String("hdfs://sr602:9000/tpcds/web_sales/ws_sold_date_sk=2452258/part-00039-5adaa592-957b-475d-95f7-64881c8e0c68.c000.snappy.parquet");
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    ParquetReaderHandler reader_handler = new ParquetReaderHandler(allocator);
    //ArrowRecordBatch record_batch = reader.readNext();
    HdfsReader hdfsReader = new HdfsReader(reader_handler, path);

    ReadThread t1 = new ReadThread(hdfsReader, allocator);
    ReadThread t2 = new ReadThread(hdfsReader, allocator);
    ReadThread t3 = new ReadThread(hdfsReader, allocator);
    ReadThread t4 = new ReadThread(hdfsReader, allocator);

    t1.start();
    t2.start();
    t3.start();
    t4.start();
    
    t1.join();
    t2.join();
    t3.join();
    t4.join();

    allocator.close();
    System.out.println("testParquetReader completed");
  }

  public static void main(String[] args) {
    try {
      testParquetReader();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  static class ReadThread extends Thread {  
    ParquetReader reader;
    List<FieldVector> vector_list;
    List<FieldVector> vector_list_1;
    Schema schema;
    String name;
    BufferAllocator allocator;

    public ReadThread(HdfsReader hdfsReader, BufferAllocator allocator) {
      int[] row_group_indices = {0};
      int[] column_indices = {0,1,2,3,4,5,6,7,8,9};
      long start_pos = 268435456;
      long end_pos = 333952154;
      //this.reader = new ParquetReader(hdfsReader, row_group_indices, column_indices, 4096);
      this.reader = new ParquetReader(hdfsReader, column_indices, start_pos, end_pos, 4096);
      try {
        this.schema = reader.getSchema();
      } catch (Exception e) {
        System.out.println(e);
      }
      this.allocator = allocator;
    }

    public void run(){  
      VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator);
      try {
        vector_list = reader.readNextVectors(schemaRoot);
        System.out.println(" ParquetReader readNextVectors done, get line: " + reader.lastReadLength());
        //System.out.println(" content is : " + schemaRoot.contentToTSVString());
      } catch (Exception e) {
        System.out.println(e);
      }
    }

    public void shutdown() {
      for (FieldVector vector : vector_list) {
        vector.close();
      }
      reader.close();
    }
  }
}

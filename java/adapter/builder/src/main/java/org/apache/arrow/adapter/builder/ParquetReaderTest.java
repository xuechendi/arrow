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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

public class ParquetReaderTest {

  public static void testParquetReader(String path, int numColumns) throws Exception {
    System.out.println("Will open file " + path);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    ParquetReaderHandler reader_handler = new ParquetReaderHandler(allocator);
    HdfsReader hdfsReader = new HdfsReader(reader_handler, path);

    ReadThread t1 = new ReadThread(hdfsReader, allocator, numColumns);

    t1.start();
    
    t1.join();

    allocator.close();
    System.out.println("testParquetReader completed");
  }

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(new Option("p", "path", true, "hdfs parquet file path, ex: hdfs://sr602:9000/tpcds/***.parquet"));
    options.addOption(new Option("c", "numColumns", true, "First N columns will be select"));
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      testParquetReader(cmd.getOptionValue("p"), Integer.parseInt(cmd.getOptionValue("c")));
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
    int record_batch_count = 0;
    int size = 0;

    public ReadThread(HdfsReader hdfsReader, BufferAllocator allocator, int numColumns) {
      int[] row_group_indices = {0};
      int[] column_indices = new int[numColumns];
      for (int i = 0; i < numColumns; i++) {
        column_indices[i] = i;
      }
      this.reader = new ParquetReader(hdfsReader, column_indices, row_group_indices, 4096);
      //this.reader = new ParquetReader(hdfsReader, column_indices, start_pos, end_pos, 4096);
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
        //vector_list = reader.readNextVectors(schemaRoot);
        //System.out.println(" ParquetReader readNextVectors done, get line: " + reader.lastReadLength());
        //System.out.println(" content is : " + schemaRoot.contentToTSVString());
        while (true) {
          ArrowRecordBatch rb = reader.readNext();
          if (rb == null) {
            break;
          }
          record_batch_count++;
          size += rb.getLength();
          rb.close();
        }
        System.out.println("Read Batches " + record_batch_count + ", total length is " + size);
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
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

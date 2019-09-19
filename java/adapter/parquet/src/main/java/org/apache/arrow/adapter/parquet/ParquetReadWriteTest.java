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

package org.apache.arrow.adapter.parquet;

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

public class ParquetReadWriteTest {

  public static void testParquetReadWrite(String path, int numColumns) throws Exception {
    System.out.println("Will open file " + path);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    ParquetReaderJniWrapper reader_handler = new ParquetReaderJniWrapper(allocator);
    ParquetWriterJniWrapper writer_handler = new ParquetWriterJniWrapper();

    int[] row_group_indices = {0};
    int[] column_indices = new int[numColumns];
    for (int i = 0; i < numColumns; i++) {
      column_indices[i] = i;
    }

    ParquetReader reader =
      new ParquetReader(reader_handler, path, column_indices, row_group_indices, 4096);
    long record_batch_count = 0;
    long size = 0;

    String w_path = path + ".output";
    try {
      Schema schema = reader.getSchema();
      System.out.println("Schema is " + schema.toString());
      ParquetWriter writer = new ParquetWriter(writer_handler, w_path, schema);
      while (true) {
        ArrowRecordBatch rb = reader.readNext();
        if (rb == null) {
          break;
        }
        record_batch_count++;
        size += rb.getLength();

        //test write
        writer.writeNext(rb);
        rb.close();
      }
      System.out.println("Read and Write Batches " + record_batch_count + ", total length is " + size);
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
    }
      //VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, allocator);
      //vector_list = reader.readNextVectors(schemaRoot);

    reader.close();
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
      testParquetReadWrite(cmd.getOptionValue("p"), Integer.parseInt(cmd.getOptionValue("c")));
    } catch (Exception e) {
      System.out.println(e);
    }
  }

}

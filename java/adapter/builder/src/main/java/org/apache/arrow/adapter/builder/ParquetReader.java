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

import java.io.IOException;
import java.lang.*;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class ParquetReader {

  private long nativeHandler;
  private long lastReadLength;
  private HdfsReader hdfsReader;

  public ParquetReader(
      HdfsReader hdfsReader, int[] row_group_indices, int[] column_indices, long batch_size) {
    this.hdfsReader = hdfsReader;
    nativeHandler = hdfsReader.openParquetFile(row_group_indices, column_indices, batch_size);
  }

  public ParquetReader(
      HdfsReader hdfsReader, int[] column_indices, long start_pos, long end_pos, long batch_size) {
    this.hdfsReader = hdfsReader;
    nativeHandler = hdfsReader.openParquetFile(column_indices, start_pos, end_pos, batch_size);
  }

  public void close() {
    hdfsReader.closeParquetFile(nativeHandler);
  }

  public ArrowRecordBatch readNext() {
    ArrowRecordBatch batch = hdfsReader.readNext(nativeHandler);
    if (batch == null) {
      return null;
    }
    lastReadLength = batch.getLength();
    return batch;
  }

  public Schema getSchema() throws IOException {
    return hdfsReader.getSchema(nativeHandler);
  }

  public List<FieldVector> readNextVectors(VectorSchemaRoot root) {
    ArrowRecordBatch batch = readNext();
    if (batch == null) {
      return null;
    }
    VectorLoader loader = new VectorLoader(root);
    loader.load(batch);
    batch.close();
    return root.getFieldVectors();
  }

  public long lastReadLength() {
    return lastReadLength;
  }
}

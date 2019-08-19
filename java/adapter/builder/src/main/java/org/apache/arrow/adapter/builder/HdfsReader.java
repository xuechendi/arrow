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

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public class HdfsReader {

  private long nativeHandler;
  private ParquetReaderHandler parquetReaderHandler;

  public HdfsReader(ParquetReaderHandler parquetReaderHandler, String path) {
    this.parquetReaderHandler = parquetReaderHandler;
    nativeHandler = parquetReaderHandler.openHdfs(path);
  }

  public void close() {
    parquetReaderHandler.closeHdfs(nativeHandler);
  }

  public long openParquetFile(int[] row_group_indices, int[] column_indices, long batch_size) {
    return parquetReaderHandler.openParquetFile(
        nativeHandler, row_group_indices, column_indices, batch_size);
  }

  public long openParquetFile(int[] column_indices, long start_pos, long end_pos, long batch_size) {
    return parquetReaderHandler.openParquetFile(
        nativeHandler, column_indices, start_pos, end_pos, batch_size);
  }

  public void closeParquetFile(long nativeHandler) {
    parquetReaderHandler.closeParquetFile(nativeHandler);
  }

  public Schema getSchema(long nativeHandler) throws IOException {
    return parquetReaderHandler.getSchema(nativeHandler);
  }

  public ArrowRecordBatch readNext(long nativeHandler) {
    return parquetReaderHandler.readNext(nativeHandler);
  }
}

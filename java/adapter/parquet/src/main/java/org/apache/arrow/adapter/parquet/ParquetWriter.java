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

import java.io.IOException;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public class ParquetWriter {

  private long parquetWriterHandler;
  private ParquetWriterJniWrapper wrapper;

  public ParquetWriter(ParquetWriterJniWrapper wrapper, String path, Schema schema)
      throws IOException {
    this.wrapper = wrapper;
    parquetWriterHandler = openParquetFile(path, schema);
  }

  public long openParquetFile(String path, Schema schema) throws IOException {
    return wrapper.openParquetFile(path, schema);
  }

  public void close() {
    wrapper.closeParquetFile(parquetWriterHandler);
  }

  public void writeNext(ArrowRecordBatch recordBatch) {
    wrapper.writeNext(parquetWriterHandler, recordBatch);
  }
}

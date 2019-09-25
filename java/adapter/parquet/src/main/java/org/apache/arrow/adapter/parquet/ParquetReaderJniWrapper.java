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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

public class ParquetReaderJniWrapper {

  private native long nativeOpenParquetReader(
      String path, int[] column_indices, int[] row_group_indices, long batch_size);
  private native long nativeOpenParquetReaderWithRange(
      String path, int[] column_indices, long start_pos, long end_pos, long batch_size);
  private native void nativeCloseParquetReader(long nativeHandler);
  private native ArrowRecordBatchBuilder nativeReadNext(long nativeHandler);
  private native byte[] nativeGetSchema(long nativeHandler);

  private BufferAllocator allocator;

  public ParquetReaderJniWrapper(BufferAllocator allocator)
      throws IOException, IllegalAccessException {
    ParquetJniUtils.getInstance();
    this.allocator = allocator;
  }

  long openParquetFile(
      String path, int[] row_group_indices, int[] column_indices, long batch_size) {
    return nativeOpenParquetReader(
        path, column_indices, row_group_indices, batch_size);
  }

  long openParquetFile(
      String path, int[] column_indices, long start_pos, long end_pos, long batch_size) {
    return nativeOpenParquetReaderWithRange(
        path, column_indices, start_pos, end_pos, batch_size);
  }

  void closeParquetFile(long nativeHandler) {
    nativeCloseParquetReader(nativeHandler);
  }

  Schema getSchema(long nativeHandler) throws IOException {
    byte[] schemaBytes = nativeGetSchema(nativeHandler);

    try (MessageChannelReader schemaReader =
           new MessageChannelReader(
                new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    }
  }

  ArrowRecordBatch readNext(long nativeHandler) {
    ArrowRecordBatchBuilder rb_builder = nativeReadNext(nativeHandler);
    ArrowRecordBatchBuilderImpl rb_builder_impl = new ArrowRecordBatchBuilderImpl(rb_builder);
    if (rb_builder == null) {
      return null;
    }
    return rb_builder_impl.build();
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import io.netty.buffer.ArrowBuf;

public class ParquetWriterJniWrapper {
  static {
    System.loadLibrary("arrow_parquet_jni");
  }
  private native long nativeOpenParquetWriter(String path, byte[] schemaBytes);
  private native void nativeCloseParquetWriter(long nativeHandler);
  private native void nativeWriteNext(
      long nativeHandler, int numRows, long[] bufAddrs, long[] bufSizes);

  public ParquetWriterJniWrapper() {
  }

  long openParquetFile(String path, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    byte[] schemaBytes = out.toByteArray(); 
    return nativeOpenParquetWriter(path, schemaBytes);
  }

  void closeParquetFile(long nativeHandler) {
    nativeCloseParquetWriter(nativeHandler);
  }

  void writeNext(long nativeHandler, ArrowRecordBatch recordBatch) {
    // convert ArrowRecordBatch to buffer List
    int numRows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }
    nativeWriteNext(nativeHandler, numRows, bufAddrs, bufSizes);
  }
}

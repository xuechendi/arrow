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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.lang.*;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ArrowBuf;

public class ParquetReadWriteTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  public void testParquetReadWrite() throws Exception {

    File testFile = testFolder.newFile("_tmpfile_ParquetWriterReaderTest");
    String path = testFile.getAbsolutePath();
    System.out.println("path is " + path);

    int numColumns = 10;
    int[] row_group_indices = {0};
    int[] column_indices = new int[numColumns];;
    for (int i = 0; i < numColumns; i++) {
      column_indices[i] = i;
    }

    ParquetReaderJniWrapper reader_handler = new ParquetReaderJniWrapper(allocator);
    ParquetWriterJniWrapper writer_handler = new ParquetWriterJniWrapper();

    Schema schema = new Schema(asList(
        field("a", new Int(8, true)),
        field("b", new Int(8, true)),
        field("c", new Int(8, true)),
        field("d", new Int(8, true)),
        field("e", new Int(8, true)),
        field("f", new Int(8, true)),
        field("g", new Int(8, true)),
        field("h", new Int(8, true)),
        field("i", new Int(8, true)),
        field("j", new Int(8, true))
    ));

    byte[] validity = new byte[] {(byte) 255, (byte) 255};
    byte[] values = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = buf(values);
    List<ArrowFieldNode> arrowFieldNodeList = new ArrayList<ArrowFieldNode>();
    List<ArrowBuf> arrowBufList = new ArrayList<ArrowBuf>();
    for (int i = 0; i < 10/*column_num*/; i++) {
      arrowFieldNodeList.add(new ArrowFieldNode(16, 0));
      arrowBufList.add(validityb);
      arrowBufList.add(valuesb);
    }
    ArrowRecordBatch batch = new ArrowRecordBatch(16, arrowFieldNodeList, arrowBufList);

    ParquetWriter writer = new ParquetWriter(writer_handler, path, schema);
    writer.writeNext(batch);
    writer.close();

    ParquetReader reader =
      new ParquetReader(reader_handler, path, column_indices, row_group_indices, 16);

    Schema readed_schema = reader.getSchema();
    assertEquals(schema.toJson(), readed_schema.toJson());

    ArrowRecordBatch rb = reader.readNext();
    assertFalse(rb == null);

    long size = rb.getLength();
    assertEquals(size, 16);
    System.out.println(rb.toString());
    reader.close();

    rb.close();
    batch.close();

    testFile.delete();
  }

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null, null), asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  private ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }
}

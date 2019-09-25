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
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class ArrowRecordBatchBuilderImpl {

  private int length;
  private ArrowRecordBatchBuilder rb_builder;

  public ArrowRecordBatchBuilderImpl (ArrowRecordBatchBuilder rb_builder) {
    this.rb_builder = rb_builder;
  }

  public ArrowRecordBatch build() {
    if (rb_builder.length == 0) {
      return null;
    }

    List<ArrowFieldNode> nodes = new ArrayList<ArrowFieldNode>();
    for (ArrowFieldNodeBuilder tmp : rb_builder.node_builders) {
      nodes.add(new ArrowFieldNode(tmp.length, tmp.nullCount));
    }

    List<ArrowBuf> buffers = new ArrayList<ArrowBuf>();
    for (ArrowBufBuilder tmp : rb_builder.buffer_builders) {
      AdaptorReferenceManager referenceManager =
        new AdaptorReferenceManager(tmp.nativeInstanceId, tmp.size);
      buffers.add(new ArrowBuf(referenceManager, null, tmp.size, tmp.memoryAddress, false));
    }
    return new ArrowRecordBatch(rb_builder.length, nodes, buffers);
  }
  
} 

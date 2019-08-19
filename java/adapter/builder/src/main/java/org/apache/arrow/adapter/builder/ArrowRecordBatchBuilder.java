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

import io.netty.buffer.ArrowBuf;
import java.lang.*;
import java.util.*;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class ArrowRecordBatchBuilder {

  private int length;
  private List<ArrowFieldNode> nodes;
  private List<ArrowBuf> buffers;

  public ArrowRecordBatchBuilder(int length, ArrowFieldNodeBuilder[] node_builders, ArrowBufBuilder[] buffer_builders) {
    this.length = length;
    this.nodes = new ArrayList<ArrowFieldNode>();
    for (ArrowFieldNodeBuilder tmp : node_builders) {
      this.nodes.add(tmp.build());
    }

    this.buffers = new ArrayList<ArrowBuf>();
    for (ArrowBufBuilder tmp : buffer_builders) {
      this.buffers.add(tmp.build());
    }
  }

  public ArrowRecordBatch build() {
    if (length == 0) {
      return null;
    }
    return new ArrowRecordBatch(length, nodes, buffers);
  }
  
} 

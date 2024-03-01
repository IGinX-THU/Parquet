/*
 * Apache Parquet MR (Incubating)
 * Copyright 2014 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.format.parquet.io;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class LocalOutputFile implements OutputFile {

  private final Path path;
  private final ByteBufferAllocator allocator;
  private final int maxBufferSize;

  public LocalOutputFile(Path file, ByteBufferAllocator allocator, int maxBufferSize) {
    this.path = file;
    this.allocator = allocator;
    this.maxBufferSize = Math.max(8 * 1024, Integer.highestOneBit(maxBufferSize));
  }

  private ByteBuffer allocate(long hint) {
    int size = (int) Math.min(maxBufferSize, hint);
    ByteBuffer byteBuffer = allocator.allocate(size);
    byteBuffer.limit(size);
    return byteBuffer;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    ByteBuffer byteBuffer = allocate(blockSizeHint);
    return new LocalPositionOutputStream(byteBuffer, StandardOpenOption.CREATE_NEW);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    ByteBuffer byteBuffer = allocate(blockSizeHint);
    return new LocalPositionOutputStream(byteBuffer, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }


  @Override
  public boolean supportsBlockSize() {
    return true;
  }

  @Override
  public long defaultBlockSize() {
    return 4 * 1024;
  }

  @Override
  public String getPath() {
    return path.toString();
  }

  private class LocalPositionOutputStream extends PositionOutputStream {

    private final ByteBuffer buffer;

    private final WritableByteChannel channel;

    private long pos = 0;

    public LocalPositionOutputStream(ByteBuffer buffer, StandardOpenOption... openOption) throws IOException {
      this.buffer = buffer;
      Set<OpenOption> optionSet = new HashSet<>(openOption.length);
      Collections.addAll(optionSet, openOption);
      optionSet.add(StandardOpenOption.WRITE);
      this.channel = Files.newByteChannel(path, optionSet);
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int data) throws IOException {
      pos++;
      if (!buffer.hasRemaining()) {
        flush();
      }
      buffer.put((byte) data);
    }

    @Override
    public void write(byte[] data) throws IOException {
      write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int off, int len) throws IOException {
      while (len > 0) {
        if (!buffer.hasRemaining()) {
          flush();
        }
        int toWrite = Math.min(len, buffer.remaining());
        buffer.put(data, off, toWrite);
        pos += toWrite;
        off += toWrite;
        len -= toWrite;
      }
    }

    @Override
    public void flush() throws IOException {
      int oldLimit = buffer.limit();
      buffer.flip();
      channel.write(buffer);
      buffer.clear();
      buffer.limit(oldLimit);
    }

    @Override
    public void close() throws IOException {
      flush();
      channel.close();
    }
  }
}

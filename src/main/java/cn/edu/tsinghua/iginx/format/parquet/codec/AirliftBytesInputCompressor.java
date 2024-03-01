/*
 * Copyright 2023 IginX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.format.parquet.codec;

import io.airlift.compress.Compressor;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AirliftBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {

  private final Compressor compressor;

  private final CompressionCodecName codecName;

  private final ByteBufferAllocator allocator;

  public AirliftBytesInputCompressor(Compressor compressor, CompressionCodecName codecName, ByteBufferAllocator allocator) {
    this.compressor = compressor;
    this.codecName = codecName;
    this.allocator = allocator;
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteBuffer ingoing = bytes.toByteBuffer();
    int maxOutputSize = compressor.maxCompressedLength(ingoing.remaining());
    ByteBuffer outgoing = allocator.allocate(maxOutputSize);

    compressor.compress(ingoing, outgoing);
    outgoing.flip();
    return BytesInput.from(outgoing);
  }

  @Override
  public CompressionCodecName getCodecName() {
    return codecName;
  }

  @Override
  public void release() {
  }
}

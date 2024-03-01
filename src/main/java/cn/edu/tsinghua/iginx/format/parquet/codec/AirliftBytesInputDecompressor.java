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

import io.airlift.compress.Decompressor;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AirliftBytesInputDecompressor
    implements CompressionCodecFactory.BytesInputDecompressor {

  private final Decompressor decompressor;
  private final ByteBufferAllocator allocator;

  public AirliftBytesInputDecompressor(Decompressor decompressor, ByteBufferAllocator allocator) {
    this.decompressor = decompressor;
    this.allocator = allocator;
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    ByteBuffer ingoing = bytes.toByteBuffer();
    ByteBuffer outgoing = allocator.allocate(uncompressedSize);

    decompressor.decompress(ingoing, outgoing);
    outgoing.flip();
    return BytesInput.from(outgoing);
  }

  @Override
  public void decompress(
      ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
    decompressor.decompress(input, output);
  }

  @Override
  public void release() {
  }
}

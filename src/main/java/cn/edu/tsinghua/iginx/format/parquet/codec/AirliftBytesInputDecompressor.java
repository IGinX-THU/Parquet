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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AirliftBytesInputDecompressor
    implements CompressionCodecFactory.BytesInputDecompressor {

  private final Decompressor decompressor;

  public AirliftBytesInputDecompressor(Decompressor decompressor) {
    this.decompressor = decompressor;
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = new byte[uncompressedSize];

    int written = decompressor.decompress(ingoing, 0, ingoing.length, outgoing, 0, outgoing.length);
    if (written != uncompressedSize) {
      throw new IOException("Non-compressed data did not have matching uncompressed sizes.");
    }
    return BytesInput.from(outgoing, 0, written);
  }

  @Override
  public void decompress(
      ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
      throws IOException {
    decompressor.decompress(input, output);
    output.position(uncompressedSize);
  }

  @Override
  public void release() {
  }
}

package org.apache.parquet.hadoop.codec;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = new byte[uncompressedSize];

    int written = Snappy.uncompress(ingoing, 0, ingoing.length, outgoing, 0);
    if (written != uncompressedSize) {
      throw new IOException("Non-compressed data did not have matching uncompressed sizes.");
    }
    return BytesInput.from(outgoing, 0, uncompressedSize);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    Snappy.uncompress(input, output);
  }

  @Override
  public void release() {
  }
}

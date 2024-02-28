package org.apache.parquet.hadoop.codec;

import io.airlift.compress.lz4.Lz4Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SegmentedLz4BytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {

  private final int segmentSize;

  public SegmentedLz4BytesInputDecompressor(int segmentSize) {
    this.segmentSize = segmentSize;
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    ByteBuffer input = bytes.toByteBuffer();
    ByteBuffer output = ByteBuffer.wrap(new byte[uncompressedSize]);
    decompress(input, input.remaining(), output, uncompressedSize);
    return BytesInput.from(output);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    Lz4Decompressor decompressor = new Lz4Decompressor();
    int offset = 0;
    while (offset < compressedSize) {
      int length = Math.min(segmentSize, compressedSize - offset);
      ByteBuffer segmentInput = input.duplicate();
      segmentInput.position(offset);
      segmentInput.limit(offset + length);
      decompressor.decompress(segmentInput, output);
      offset += length;
    }
    output.position(uncompressedSize);
  }

  @Override
  public void release() {

  }
}

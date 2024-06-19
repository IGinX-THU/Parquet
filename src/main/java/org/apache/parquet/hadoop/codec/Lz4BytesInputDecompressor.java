package org.apache.parquet.hadoop.codec;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Lz4BytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {

  private final static LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
  private final LZ4FastDecompressor compressor = lz4Factory.fastDecompressor();

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = new byte[uncompressedSize];

    compressor.decompress(ingoing, 0, outgoing, 0, uncompressedSize);
    return BytesInput.from(outgoing, 0, uncompressedSize);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    input.limit(input.position() + compressedSize);
    output.limit(output.position() + uncompressedSize);
    compressor.decompress(input, output);
  }

  @Override
  public void release() {
  }
}

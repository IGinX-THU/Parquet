package org.apache.parquet.hadoop.codec;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class Lz4BytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {

  private final static LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
  private final LZ4Compressor compressor = lz4Factory.fastCompressor();

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    int maxOutputSize = compressor.maxCompressedLength(Math.toIntExact(bytes.size()));
    byte[] outgoing = new byte[maxOutputSize];

    int compressedSize = compressor.compress(ingoing, 0, ingoing.length, outgoing, 0, outgoing.length);
    return BytesInput.from(outgoing, 0, compressedSize);
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.LZ4_RAW;
  }

  @Override
  public void release() {
  }
}

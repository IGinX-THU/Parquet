package org.apache.parquet.hadoop.codec;

import io.airlift.compress.snappy.SnappyCompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Map;

public class SnappyBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    int maxOutputSize = Snappy.maxCompressedLength(Math.toIntExact(bytes.size()));
    byte[] outgoing = new byte[maxOutputSize];
    int compressedSize  = Snappy.compress(ingoing, 0, ingoing.length, outgoing, 0);
    return BytesInput.from(outgoing, 0, compressedSize);
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.SNAPPY;
  }

  @Override
  public void release() {
  }
}

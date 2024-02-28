package org.apache.parquet.hadoop.codec;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class BuiltinGzipBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    GZIPInputStream gzipInputStream = new GZIPInputStream(bytes.toInputStream());
    return BytesInput.from(gzipInputStream, uncompressedSize);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    BytesInput bytesInput = decompress(BytesInput.from(input), uncompressedSize);
    output.put(bytesInput.toByteArray());
    output.position(uncompressedSize);
  }

  @Override
  public void release() {
  }
}
package cn.edu.tsinghua.iginx.format.parquet.codec;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.brotli.dec.BrotliInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BrotliBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    try (InputStream inputStream = bytes.toInputStream();
         BrotliInputStream brotliInputStream = new BrotliInputStream(inputStream)) {
      return BytesInput.from(brotliInputStream, uncompressedSize);
    }
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    output.clear();
    BytesInput bytesInput = decompress(BytesInput.from(input), uncompressedSize);
    output.put(bytesInput.toByteArray());
    output.position(uncompressedSize);
  }

  @Override
  public void release() {
  }
}

package org.apache.parquet.hadoop.codec;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ZstdJniBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    try (InputStream input = bytes.toInputStream();
         ZstdInputStream zstdInputStream = new ZstdInputStream(input, RecyclingBufferPool.INSTANCE)) {
      BytesInput result = BytesInput.from(zstdInputStream, uncompressedSize);
      return BytesInput.copy(result);
    }
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    Zstd.decompress(output, input);
  }

  @Override
  public void release() {
    // Nothing to do here since we release resources where we create them
  }
}

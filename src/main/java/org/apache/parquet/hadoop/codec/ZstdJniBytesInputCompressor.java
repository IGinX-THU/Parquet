package org.apache.parquet.hadoop.codec;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdJniBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {
  private final int level;

  public ZstdJniBytesInputCompressor(int level) {
    this.level = level;
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = new byte[Math.toIntExact(Zstd.compressBound(ingoing.length))];

    long written = Zstd.compressByteArray(outgoing, 0, outgoing.length, ingoing, 0, ingoing.length, level);

    if(Zstd.isError(written)) {
      throw new IOException("Error during Zstd compression: " + Zstd.getErrorName(written));
    }

    return BytesInput.from(outgoing, 0, Math.toIntExact(written));
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.ZSTD;
  }

  @Override
  public void release() {
  }
}

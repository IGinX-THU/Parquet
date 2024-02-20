package cn.edu.tsinghua.iginx.format.parquet.codec;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdJniBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {
  private final int level;
  private final int workers;

  public ZstdJniBytesInputCompressor(int level, int workers) {
    this.level = level;
    this.workers = workers;
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream((int) bytes.size());
    try (ZstdOutputStream zstdStream = new ZstdOutputStream(stream, RecyclingBufferPool.INSTANCE, level)) {
      zstdStream.setWorkers(workers);
      bytes.writeAllTo(zstdStream);
    }
    return BytesInput.from(stream);
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.ZSTD;
  }

  @Override
  public void release() {
  }
}

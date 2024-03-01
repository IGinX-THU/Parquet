package cn.edu.tsinghua.iginx.format.parquet.codec;

import cn.edu.tsinghua.iginx.format.parquet.io.ByteBufferOutputStream;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ZstdJniBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {
  private final int level;
  private final int workers;

  private final ByteBufferAllocator allocator;

  public ZstdJniBytesInputCompressor(int level, int workers, ByteBufferAllocator allocator) {
    this.level = level;
    this.workers = workers;
    this.allocator = allocator;
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteBufferOutputStream temp = new ByteBufferOutputStream((int) Zstd.compressBound(bytes.size()), allocator);
    try (ZstdOutputStream zstdOutputStream = new ZstdOutputStream(temp, RecyclingBufferPool.INSTANCE, level)) {
      zstdOutputStream.setWorkers(workers);
      bytes.writeAllTo(zstdOutputStream);
    }
    return BytesInput.from(temp.toByteBuffer());
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.ZSTD;
  }

  @Override
  public void release() {
  }
}

package cn.edu.tsinghua.iginx.format.parquet.codec;

import cn.edu.tsinghua.iginx.format.parquet.io.ByteBufferOutputStream;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class BuiltinGzipBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {

  private final ByteBufferAllocator allocator;

  public BuiltinGzipBytesInputCompressor(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteBufferOutputStream temp = new ByteBufferOutputStream((int) bytes.size(), allocator);
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(temp)) {
      bytes.writeAllTo(gzipOutputStream);
    }
    return BytesInput.from(temp.toByteBuffer());
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.GZIP;
  }

  @Override
  public void release() {
  }


}

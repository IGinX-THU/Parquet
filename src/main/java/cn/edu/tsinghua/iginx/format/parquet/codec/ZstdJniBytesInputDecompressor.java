package cn.edu.tsinghua.iginx.format.parquet.codec;

import com.github.luben.zstd.ZstdBufferDecompressingStream;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ZstdJniBytesInputDecompressor implements CompressionCodecFactory.BytesInputDecompressor {

  private final ByteBufferAllocator allocator;

  public ZstdJniBytesInputDecompressor(ByteBufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
    ByteBuffer ingoing = bytes.toByteBuffer();
    ByteBuffer outgoing = allocator.allocate(uncompressedSize);
    try (ZstdBufferDecompressingStream zstdStream = new ZstdBufferDecompressingStream(ingoing)) {
      zstdStream.read(outgoing);
    }
    outgoing.flip();
    return BytesInput.from(outgoing);
  }

  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
    try (ZstdBufferDecompressingStream zstdStream = new ZstdBufferDecompressingStream(input)) {
      zstdStream.read(output);
    }
  }

  @Override
  public void release() {
    // Nothing to do here since we release resources where we create them
  }
}

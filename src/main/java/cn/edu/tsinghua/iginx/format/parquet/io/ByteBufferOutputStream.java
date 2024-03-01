package cn.edu.tsinghua.iginx.format.parquet.io;

import org.apache.parquet.bytes.ByteBufferAllocator;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
  private final ByteBufferAllocator allocator;
  private final int initialSize;

  private ByteBuffer buffer = null;

  public ByteBufferOutputStream(int initialSize, ByteBufferAllocator allocator) {
    this.allocator = allocator;
    this.initialSize = initialSize;
  }

  private void ensureRemaining(int remaining) {
    if (buffer == null) {
      buffer = allocator.allocate(initialSize);
    }
    if (buffer.remaining() < remaining) {
      ByteBuffer newBuffer = allocator.allocate(buffer.capacity() * 2);
      buffer.flip();
      newBuffer.put(buffer);
      allocator.release(buffer);
      buffer = newBuffer;
    }
  }

  @Override
  public void write(int b) throws IOException {
    ensureRemaining(1);
    buffer.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureRemaining(len);
    buffer.put(b, off, len);
  }

  public ByteBuffer toByteBuffer() {
    buffer.flip();
    ByteBuffer result = buffer;
    buffer = null;
    return result;
  }

}

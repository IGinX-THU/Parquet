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
    byte[] ingoing = bytes.toByteArray();
    byte[] outgoing = new byte[uncompressedSize];

    long written = Zstd.decompressByteArray(outgoing, 0, outgoing.length, ingoing, 0, ingoing.length);

    if (Zstd.isError(written)) {
      throw new IOException("Error during Zstd decompression: " + Zstd.getErrorName(written));
    }

    if(written != uncompressedSize) {
      throw new IOException("Non-compressed data did not have matching uncompressed sizes.");
    }

    return BytesInput.from(outgoing, 0, uncompressedSize);
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

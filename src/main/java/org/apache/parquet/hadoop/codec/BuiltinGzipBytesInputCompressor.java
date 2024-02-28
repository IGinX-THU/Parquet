package org.apache.parquet.hadoop.codec;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class BuiltinGzipBytesInputCompressor implements CompressionCodecFactory.BytesInputCompressor {
  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    ByteArrayOutputStream outgoing = new ByteArrayOutputStream((int) bytes.size());
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outgoing)) {
      bytes.writeAllTo(gzipOutputStream);
    }
    return BytesInput.from(outgoing.toByteArray());
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.GZIP;
  }

  @Override
  public void release() {
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import cn.edu.tsinghua.iginx.format.parquet.codec.*;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CodecFactory implements CompressionCodecFactory {

  public static final int DEFAULT_LZ4_SEGMENT_SIZE = 256 * 1024;
  public static final int DEFAULT_ZSTD_LEVEL = 3;
  public static final int DEFAULT_ZSTD_WORKERS = 0;

  private final int lz4SegmentSize;
  private final int zstdLevel;
  private final int zstdWorkers;

  public CodecFactory() {
    this(DEFAULT_LZ4_SEGMENT_SIZE, DEFAULT_ZSTD_LEVEL, DEFAULT_ZSTD_WORKERS);
  }

  public CodecFactory(int lz4SegmentSize, int zstdLevel, int zstdWorkers) {
    this.lz4SegmentSize = lz4SegmentSize;
    this.zstdLevel = zstdLevel;
    this.zstdWorkers = zstdWorkers;
  }

  public static BytesCompressor wrap(CompressionCodecFactory.BytesInputCompressor compressor) {
    return new BytesCompressor() {
      @Override
      public BytesInput compress(BytesInput bytes) throws IOException {
        return compressor.compress(bytes);
      }

      @Override
      public CompressionCodecName getCodecName() {
        return compressor.getCodecName();
      }

      @Override
      public void release() {
        compressor.release();
      }
    };
  }

  public static BytesDecompressor wrap(CompressionCodecFactory.BytesInputDecompressor decompressor) {
    return new BytesDecompressor() {
      @Override
      public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
        return decompressor.decompress(bytes, uncompressedSize);
      }

      @Override
      public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
        decompressor.decompress(input, compressedSize, output, uncompressedSize);
      }

      @Override
      public void release() {
        decompressor.release();
      }
    };
  }

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
    return createCompressor(codecName);
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
    return createDecompressor(codecName);
  }

  @Override
  public void release() {
  }

  protected BytesInputCompressor createCompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return new NoopBytesInputCompressor();
      case SNAPPY:
        return new AirliftBytesInputCompressor(new SnappyCompressor(), codecName);
      case GZIP:
        return new BuiltinGzipBytesInputCompressor();
      case LZO:
        return new AirliftBytesInputCompressor(new LzoCompressor(), codecName);
      case ZSTD:
        return new ZstdJniBytesInputCompressor(zstdLevel, zstdWorkers);
      case LZ4_RAW:
        return new AirliftBytesInputCompressor(new Lz4Compressor(), codecName);
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codecName);
    }
  }

  protected BytesInputDecompressor createDecompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return new NoopBytesInputDecompressor();
      case SNAPPY:
        return new AirliftBytesInputDecompressor(new SnappyDecompressor());
      case GZIP:
        return new BuiltinGzipBytesInputDecompressor();
      case LZO:
        return new AirliftBytesInputDecompressor(new LzoDecompressor());
      case BROTLI:
        return new BrotliBytesInputDecompressor();
      case LZ4:
        return new SegmentedLz4BytesInputDecompressor(lz4SegmentSize);
      case ZSTD:
        return new ZstdJniBytesInputDecompressor();
      case LZ4_RAW:
        return new AirliftBytesInputDecompressor(new Lz4Decompressor());
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codecName);
    }
  }

  @Deprecated
  public static abstract class BytesCompressor implements CompressionCodecFactory.BytesInputCompressor {
    public abstract BytesInput compress(BytesInput bytes) throws IOException;

    public abstract CompressionCodecName getCodecName();

    public abstract void release();
  }

  @Deprecated
  public static abstract class BytesDecompressor implements CompressionCodecFactory.BytesInputDecompressor {
    public abstract BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException;

    public abstract void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException;

    public abstract void release();
  }
}

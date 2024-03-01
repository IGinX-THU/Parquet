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
package cn.edu.tsinghua.iginx.format.parquet.codec;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DefaultCodecFactory implements CompressionCodecFactory {

  public static final int DEFAULT_LZ4_SEGMENT_SIZE = 256 * 1024;
  public static final int DEFAULT_ZSTD_LEVEL = 3;
  public static final int DEFAULT_ZSTD_WORKERS = 0;

  private final ByteBufferAllocator allocator;
  private final int lz4SegmentSize;
  private final int zstdLevel;
  private final int zstdWorkers;

  public DefaultCodecFactory() {
    this(new HeapByteBufferAllocator(), DEFAULT_LZ4_SEGMENT_SIZE, DEFAULT_ZSTD_LEVEL, DEFAULT_ZSTD_WORKERS);
  }

  public DefaultCodecFactory(ByteBufferAllocator allocator, int lz4SegmentSize, int zstdLevel, int zstdWorkers) {
    this.allocator = Objects.requireNonNull(allocator);
    this.lz4SegmentSize = lz4SegmentSize;
    this.zstdLevel = zstdLevel;
    this.zstdWorkers = zstdWorkers;
  }

  public static CodecFactory.BytesCompressor wrap(CompressionCodecFactory.BytesInputCompressor compressor) {
    return new CodecFactory.BytesCompressor() {
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

  public static CodecFactory.BytesDecompressor wrap(CompressionCodecFactory.BytesInputDecompressor decompressor) {
    return new CodecFactory.BytesDecompressor() {
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
        return new AirliftBytesInputCompressor(new SnappyCompressor(), codecName, allocator);
      case GZIP:
        return new BuiltinGzipBytesInputCompressor(allocator);
      case LZO:
        return new AirliftBytesInputCompressor(new LzoCompressor(), codecName, allocator);
      case ZSTD:
        return new ZstdJniBytesInputCompressor(zstdLevel, zstdWorkers, allocator);
      case LZ4_RAW:
        return new AirliftBytesInputCompressor(new Lz4Compressor(), codecName, allocator);
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codecName);
    }
  }

  protected BytesInputDecompressor createDecompressor(CompressionCodecName codecName) {
    switch (codecName) {
      case UNCOMPRESSED:
        return new NoopBytesInputDecompressor();
      case SNAPPY:
        return new AirliftBytesInputDecompressor(new SnappyDecompressor(), allocator);
      case GZIP:
        return new BuiltinGzipBytesInputDecompressor();
      case LZO:
        return new AirliftBytesInputDecompressor(new LzoDecompressor(), allocator);
      case BROTLI:
        return new BrotliBytesInputDecompressor();
      case LZ4:
        return new SegmentedLz4BytesInputDecompressor(lz4SegmentSize, allocator);
      case ZSTD:
        return new ZstdJniBytesInputDecompressor(allocator);
      case LZ4_RAW:
        return new AirliftBytesInputDecompressor(new Lz4Decompressor(), allocator);
      default:
        throw new IllegalArgumentException("Unsupported codec: " + codecName);
    }
  }

}

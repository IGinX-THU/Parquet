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
package cn.edu.tsinghua.iginx.format.parquet;

import cn.edu.tsinghua.iginx.format.parquet.api.RecordDematerializer;
import cn.edu.tsinghua.iginx.format.parquet.codec.DefaultCodecFactory;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ExportedParquetRecordWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {
  public static final String OBJECT_MODEL_NAME_PROP = "writer.model.name";

  protected final ExportedParquetRecordWriter<T> recordWriter;

  protected ParquetWriter(ExportedParquetRecordWriter<T> recordWriter) throws IOException {
    this.recordWriter = Objects.requireNonNull(recordWriter);
  }

  public void write(T object) throws IOException {
    recordWriter.write(object);
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return recordWriter.getDataSize();
  }

  @Override
  public void close() throws IOException {
    recordWriter.close();
  }

  /**
   * the abstract builder for the ParquetWriter
   *
   * @param <T>       the type of the records written to the file
   * @param <BUILDER> the type of the concrete builder
   */
  public abstract static class Builder<T, WRITER extends ParquetWriter<T>, BUILDER extends Builder<T, WRITER, BUILDER>> {
    private final ParquetProperties.Builder parquetPropertiesBuilder = ParquetProperties.builder();
    private final FileEncryptionProperties encryptionProperties = null;
    private CompressionCodecFactory codecFactory = new DefaultCodecFactory();
    private CompressionCodecName codecName = CompressionCodecName.UNCOMPRESSED;
    private long rowGroupSize = 8 * 1024 * 1024; // 8MB
    private int maxPaddingSize = 128 * 1024 * 1024; // 128MB
    private boolean enableValidation = true;
    private boolean enableOverwrite = false;


    /**
     * used for method chaining
     *
     * @return this builder for method chaining
     */
    protected abstract BUILDER self();

    /**
     * get the record dematerializer of the coming records
     *
     * @return the record dematerializer
     * @throws IOException if the dematerializer cannot be created
     */
    protected abstract RecordDematerializer<T> dematerializer() throws IOException;

    /**
     * build the parquet writer
     *
     * @return the built parquet writer
     */
    public abstract WRITER build() throws IOException;

    /**
     * build record writer for constructing the {@link ParquetWriter}
     *
     * @param file   the file to write to
     * @param schema the schema of the records
     * @param extra  the extra metadata of the file
     * @return the built record writer
     */
    protected ExportedParquetRecordWriter<T> build(OutputFile file, MessageType schema, Map<String, String> extra) throws IOException {
      Objects.requireNonNull(file);
      Objects.requireNonNull(schema);
      Objects.requireNonNull(extra);

      ParquetProperties parquetProperties = parquetPropertiesBuilder.build();
      RecordDematerializer<T> dematerializer = Objects.requireNonNull(dematerializer());
      ParquetFileWriter.Mode mode = enableOverwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
      BytesInputCompressor compressor = codecFactory.getCompressor(codecName);

      ParquetFileWriter fileWriter = new ParquetFileWriter(file, schema, mode, rowGroupSize, maxPaddingSize, parquetProperties.getColumnIndexTruncateLength(),
          parquetProperties.getStatisticsTruncateLength(), parquetProperties.getPageWriteChecksumEnabled(), encryptionProperties);
      fileWriter.start();
      return new ExportedParquetRecordWriter<>(
          fileWriter,
          dematerializer,
          schema,
          extra,
          rowGroupSize,
          compressor,
          enableValidation,
          parquetProperties);
    }

    public BUILDER withOverwrite(boolean enableOverwrite) {
      this.enableOverwrite = enableOverwrite;
      return self();
    }

    public BUILDER withAllocator(ByteBufferAllocator allocator) {
      Objects.requireNonNull(allocator);
      parquetPropertiesBuilder.withAllocator(allocator);
      return self();
    }

    public BUILDER withCodec(CompressionCodecName compressionCodecName) {
      Objects.requireNonNull(compressionCodecName);
      this.codecName = compressionCodecName;
      return self();
    }

    public BUILDER withCodecFactory(CompressionCodecFactory codecFactory) {
      Objects.requireNonNull(codecFactory);
      this.codecFactory = codecFactory;
      return self();
    }

    /**
     * Set the Parquet format row group size.
     *
     * @param rowGroupSize a long size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withRowGroupSize(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return self();
    }

    /**
     * Set the Parquet format page size.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withPageSize(int pageSize) {
      parquetPropertiesBuilder.withPageSize(pageSize);
      return self();
    }

    /**
     * Set the Parquet format dictionary page size.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withDictionaryPageSize(int dictionaryPageSize) {
      parquetPropertiesBuilder.withDictionaryPageSize(dictionaryPageSize);
      return self();
    }

    /**
     * Enable or disable dictionary encoding.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public BUILDER withDictionaryEncoding(boolean enableDictionary) {
      parquetPropertiesBuilder.withDictionaryEncoding(enableDictionary);
      return self();
    }

    /**
     * Enable or disable dictionary encoding for the specified column.
     *
     * @param columnPath       the path of the column (dot-string)
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public BUILDER withDictionaryEncoding(String columnPath, boolean enableDictionary) {
      Objects.requireNonNull(columnPath);
      parquetPropertiesBuilder.withDictionaryEncoding(columnPath, enableDictionary);
      return self();
    }

    /**
     * Set the Parquet format max padding size.
     *
     * @param maxPaddingSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withMaxPaddingSize(int maxPaddingSize) {
      this.maxPaddingSize = maxPaddingSize;
      return self();
    }

    public BUILDER withValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return self();
    }

    /**
     * Set the {@link ParquetProperties.WriterVersion format version}.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public BUILDER withWriterVersion(ParquetProperties.WriterVersion version) {
      parquetPropertiesBuilder.withWriterVersion(version);
      return self();
    }
  }

}

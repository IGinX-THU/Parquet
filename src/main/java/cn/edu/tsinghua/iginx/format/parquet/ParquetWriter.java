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

import org.apache.parquet.ParquetWriteOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordDematerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {
  public static final String OBJECT_MODEL_NAME_PROP = "writer.model.name";

  protected final ParquetRecordWriter<T> recordWriter;

  protected ParquetWriter(ParquetRecordWriter<T> recordWriter) throws IOException {
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

    private final ParquetWriteOptions.Builder optionsBuilder = ParquetWriteOptions.builder();

    protected Builder() {
    }

    /**
     * used for method chaining
     *
     * @return this builder for method chaining
     */
    protected abstract BUILDER self();

    /**
     * build record writer for constructing the {@link ParquetWriter}
     *
     * @param file the file to write to
     * @return the built record writer
     */
    protected ParquetRecordWriter<T> build(OutputFile file) throws IOException {
      Objects.requireNonNull(file);

      ParquetWriteOptions options = optionsBuilder.build();
      ParquetFileWriter fileWriter = new ParquetFileWriter(file, options);

      RecordDematerializer<T> dematerializer = Objects.requireNonNull(getDematerializer());
      return new ParquetRecordWriter<>(fileWriter, dematerializer, options);
    }

    /**
     * get the record dematerializer of the coming records
     *
     * @return the record dematerializer
     * @throws IOException if the dematerializer cannot be created
     */
    protected abstract RecordDematerializer<T> getDematerializer() throws IOException;

    /**
     * build the parquet writer
     *
     * @return the built parquet writer
     */
    public abstract WRITER build() throws IOException;

    public BUILDER withOverwrite(boolean enableOverwrite) {
      optionsBuilder.withOverwrite(enableOverwrite);
      return self();
    }

    public BUILDER withAllocator(ByteBufferAllocator allocator) {
      optionsBuilder.asParquetPropertiesBuilder().withAllocator(allocator);
      return self();
    }

    public BUILDER withCompressionCodec(CompressionCodecName compressionCodecName) {
      CompressionCodecFactory.BytesInputCompressor compressor = new CodecFactory().getCompressor(compressionCodecName);
      optionsBuilder.withCompressor(compressor);
      return self();
    }

    /**
     * Set the Parquet format row group size.
     *
     * @param rowGroupSize a long size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withRowGroupSize(long rowGroupSize) {
      optionsBuilder.withRowGroupSize(rowGroupSize);
      return self();
    }

    /**
     * Set the Parquet format page size.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withPageSize(int pageSize) {
      optionsBuilder.asParquetPropertiesBuilder().withPageSize(pageSize);
      return self();
    }

    /**
     * Set the Parquet format dictionary page size.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public BUILDER withDictionaryPageSize(int dictionaryPageSize) {
      optionsBuilder.asParquetPropertiesBuilder().withDictionaryPageSize(dictionaryPageSize);
      return self();
    }

    /**
     * Enable or disable dictionary encoding.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public BUILDER withDictionaryEncoding(boolean enableDictionary) {
      optionsBuilder.asParquetPropertiesBuilder().withDictionaryEncoding(enableDictionary);
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
      optionsBuilder.asParquetPropertiesBuilder().withDictionaryEncoding(columnPath, enableDictionary);
      return self();
    }

    public BUILDER withValidation(boolean enableValidation) {
      optionsBuilder.withValidation(enableValidation);
      return self();
    }

    /**
     * Set the {@link ParquetProperties.WriterVersion format version}.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public BUILDER withWriterVersion(ParquetProperties.WriterVersion version) {
      optionsBuilder.asParquetPropertiesBuilder().withWriterVersion(version);
      return self();
    }
  }

}

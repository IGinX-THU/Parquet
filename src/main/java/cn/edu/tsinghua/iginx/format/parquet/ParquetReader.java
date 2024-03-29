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

import cn.edu.tsinghua.iginx.format.parquet.codec.DefaultCodecFactory;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ExportedParquetRecordReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class ParquetReader<T> implements Closeable {

  private final ExportedParquetRecordReader<T> recordReader;

  protected ParquetReader(ExportedParquetRecordReader<T> recordReader) {
    this.recordReader = Objects.requireNonNull(recordReader);
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  /**
   * Read the next record from the file
   *
   * @return the next record or null if finished
   * @throws IOException if there is an error while reading
   */
  public T read() throws IOException {
    if (!recordReader.nextKeyValue()) {
      return null;
    }
    return recordReader.getCurrentValue();
  }

  /**
   * @return the row index of the last read row. If no row has been processed, returns -1.
   */
  public long getCurrentRowIndex() {
    return recordReader.getCurrentRowIndex();
  }

  public abstract static class Builder<T, READER extends ParquetReader<T>, BUILDER extends ParquetReader.Builder<T, READER, BUILDER>> {

    private final ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
    private Function<MessageType, MessageType> schemaConverter = Function.identity();

    protected Builder() {
      optionsBuilder.withCodecFactory(new DefaultCodecFactory());
    }

    /**
     * @return this builder for method chaining
     */
    protected abstract BUILDER self();

    /**
     * get the record materializer of the coming records
     *
     * @param schema the requested schema
     * @param extra  extra metadata from the file
     * @return the record materializer
     * @throws IOException if the materializer cannot be created
     */
    protected abstract RecordMaterializer<T> materializer(MessageType schema, Map<String, String> extra) throws IOException;

    public abstract READER build() throws IOException;

    protected ParquetMetadata readFooter(InputFile file) throws IOException {
      Objects.requireNonNull(file);

      try (SeekableInputStream in = file.newStream()) {
        return ParquetFileReader.readFooter(file, optionsBuilder.build(), in);
      }
    }

    protected ExportedParquetRecordReader<T> build(InputFile file, ParquetMetadata footer) throws IOException {
      Objects.requireNonNull(file);
      Objects.requireNonNull(footer);

      ParquetReadOptions options = optionsBuilder.build();

      ParquetFileReader reader = new ParquetFileReader(file, footer, options);
      ParquetMetadata metadata = reader.getFooter();
      MessageType schema = metadata.getFileMetaData().getSchema();
      MessageType requestedSchema = schemaConverter.apply(schema);

      RecordMaterializer<T> recordMaterializer = materializer(requestedSchema, metadata.getFileMetaData().getKeyValueMetaData());
      return new ExportedParquetRecordReader<>(recordMaterializer, reader, requestedSchema, options);
    }

    public BUILDER withFilter(FilterCompat.Filter filter) {
      optionsBuilder.withRecordFilter(filter);
      return self();
    }

    public BUILDER withAllocator(ByteBufferAllocator allocator) {
      optionsBuilder.withAllocator(allocator);
      return self();
    }

    public BUILDER useDictionaryFilter(boolean useDictionaryFiltering) {
      optionsBuilder.useDictionaryFilter(useDictionaryFiltering);
      return self();
    }

    public BUILDER useStatsFilter(boolean useStatsFiltering) {
      optionsBuilder.useStatsFilter(useStatsFiltering);
      return self();
    }

    public BUILDER useRecordFilter(boolean useRecordFiltering) {
      optionsBuilder.useRecordFilter(useRecordFiltering);
      return self();
    }

    public BUILDER useColumnIndexFilter(boolean useColumnIndexFilter) {
      optionsBuilder.useColumnIndexFilter(useColumnIndexFilter);
      return self();
    }

    public BUILDER withFileRange(long rangeStart, long rangeEnd) {
      optionsBuilder.withRange(rangeStart, rangeEnd);
      return self();
    }

    public BUILDER withSchemaConverter(Function<MessageType, MessageType> schemaConverter) {
      this.schemaConverter = schemaConverter;
      return self();
    }

    public BUILDER withCodecFactory(CompressionCodecFactory codecFactory) {
      optionsBuilder.withCodecFactory(codecFactory);
      return self();
    }
  }
}

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

import org.apache.parquet.ParquetWriteOptions;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordDematerializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class ParquetRecordWriter<T> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordWriter.class);

  private final ParquetFileWriter parquetFileWriter;
  private final RecordDematerializer<T> recordDematerializer;
  private final MessageType schema;
  private final long rowGroupSizeThreshold;
  private final CodecFactory.BytesCompressor compressor;
  private final boolean validating;
  private final ParquetProperties props;
  private final InternalFileEncryptor fileEncryptor;
  private long nextRowGroupSize;
  private boolean closed;
  private long recordCount = 0;
  private long recordCountForNextMemCheck;
  private long lastRowGroupEndPos = 0;
  private ColumnWriteStore columnStore;
  private ColumnChunkPageWriteStore pageStore;
  private RecordConsumer recordConsumer;
  private int rowGroupOrdinal;

  public ParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      RecordDematerializer<T> recordDematerializer,
      ParquetWriteOptions options)
      throws IOException {
    parquetFileWriter.start();

    this.parquetFileWriter = parquetFileWriter;
    this.recordDematerializer =
        Objects.requireNonNull(recordDematerializer, "writeSupport cannot be null");
    this.schema = recordDematerializer.getSchema();
    this.rowGroupSizeThreshold = options.getRowGroupSize();
    this.nextRowGroupSize = rowGroupSizeThreshold;
    this.compressor = CodecFactory.wrap(options.getCompressor());
    this.validating = options.isEnableValidation();
    this.props = options.getParquetProperties();
    this.fileEncryptor = parquetFileWriter.getEncryptor();
    this.rowGroupOrdinal = 0;
    initStore();
    recordCountForNextMemCheck = props.getMinRowCountForPageSizeCheck();
  }

  private void initStore() {
    ColumnChunkPageWriteStore columnChunkPageWriteStore =
        new ColumnChunkPageWriteStore(
            compressor,
            schema,
            props.getAllocator(),
            props.getColumnIndexTruncateLength(),
            props.getPageWriteChecksumEnabled(),
            fileEncryptor,
            rowGroupOrdinal);
    pageStore = columnChunkPageWriteStore;

    columnStore = props.newColumnWriteStore(schema, pageStore, columnChunkPageWriteStore);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    this.recordConsumer = columnIO.getRecordWriter(columnStore);
    recordDematerializer.setRecordConsumer(recordConsumer);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      flushRowGroupToStore();
      parquetFileWriter.end(schema, recordDematerializer.getExtraMetaData());
      closed = true;
      this.compressor.release();
    }
  }

  public void write(T value) throws IOException {
    recordDematerializer.write(value);
    ++recordCount;
    checkBlockSizeReached();
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return lastRowGroupEndPos + columnStore.getBufferedSize();
  }

  public ParquetMetadata getFooter() {
    return parquetFileWriter.getFooter();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount
        >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so
      // let's not do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.debug(
            "mem size {} > {}: flushing {} records to disk.",
            memSize,
            nextRowGroupSize,
            recordCount);
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck =
            min(
                max(props.getMinRowCountForPageSizeCheck(), recordCount / 2),
                props.getMaxRowCountForPageSizeCheck());
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
        recordCountForNextMemCheck =
            min(
                max(
                    props.getMinRowCountForPageSizeCheck(),
                    (recordCount + (long) (nextRowGroupSize / ((float) recordSize)))
                        / 2), // will check halfway
                recordCount
                    + props.getMaxRowCountForPageSizeCheck() // will not look more than max records
                // ahead
            );
        LOG.debug(
            "Checked mem at {} will check again at: {}", recordCount, recordCountForNextMemCheck);
      }
    }
  }

  private void flushRowGroupToStore() throws IOException {
    recordConsumer.flush();
    LOG.debug(
        "Flushing mem columnStore to file. allocated memory: {}", columnStore.getAllocatedSize());
    if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
      LOG.warn("Too much memory used: {}", columnStore.memUsageString());
    }

    if (recordCount > 0) {
      rowGroupOrdinal++;
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
      this.nextRowGroupSize = min(parquetFileWriter.getNextRowGroupSize(), rowGroupSizeThreshold);
    }

    columnStore = null;
    pageStore = null;
  }
}

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
package org.apache.parquet.local;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains several pages, which are yielded
   * one by one in order.
   *
   * <p>This implementation is provided with a list of pages, each of which is decompressed and
   * passed through.
   */
  static final class ColumnChunkPageReader implements PageReader {

    private final BytesInputDecompressor decompressor;
    private final long valueCount;
    private final Queue<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    // null means no page synchronization is required; firstRowIndex will not be returned by the
    // pages
    private final OffsetIndex offsetIndex;
    private final long rowCount;
    private int pageIndex = 0;

    private final BlockCipher.Decryptor blockDecryptor;
    private final byte[] dataPageAAD;
    private final byte[] dictionaryPageAAD;

    ColumnChunkPageReader(
        BytesInputDecompressor decompressor,
        List<DataPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        OffsetIndex offsetIndex,
        long rowCount,
        BlockCipher.Decryptor blockDecryptor,
        byte[] fileAAD,
        int rowGroupOrdinal,
        int columnOrdinal) {
      this.decompressor = decompressor;
      this.compressedPages = new ArrayDeque<DataPage>(compressedPages);
      this.compressedDictionaryPage = compressedDictionaryPage;
      long count = 0;
      for (DataPage p : compressedPages) {
        count += p.getValueCount();
      }
      this.valueCount = count;
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;

      this.blockDecryptor = blockDecryptor;

      if (null != blockDecryptor) {
        dataPageAAD =
            AesCipher.createModuleAAD(
                fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
        dictionaryPageAAD =
            AesCipher.createModuleAAD(
                fileAAD, ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
      } else {
        dataPageAAD = null;
        dictionaryPageAAD = null;
      }
    }

    private int getPageOrdinal(int currentPageIndex) {
      if (null == offsetIndex) {
        return currentPageIndex;
      }

      return offsetIndex.getPageOrdinal(currentPageIndex);
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    @Override
    public DataPage readPage() {
      final DataPage compressedPage = compressedPages.poll();
      if (compressedPage == null) {
        return null;
      }
      final int currentPageIndex = pageIndex++;

      if (null != blockDecryptor) {
        AesCipher.quickUpdatePageAAD(dataPageAAD, getPageOrdinal(currentPageIndex));
      }

      return compressedPage.accept(
          new DataPage.Visitor<DataPage>() {
            @Override
            public DataPage visit(DataPageV1 dataPageV1) {
              try {
                BytesInput bytes = dataPageV1.getBytes();
                if (null != blockDecryptor) {
                  bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
                }
                BytesInput decompressed =
                    decompressor.decompress(bytes, dataPageV1.getUncompressedSize());

                final DataPageV1 decompressedPage;
                if (offsetIndex == null) {
                  decompressedPage =
                      new DataPageV1(
                          decompressed,
                          dataPageV1.getValueCount(),
                          dataPageV1.getUncompressedSize(),
                          dataPageV1.getStatistics(),
                          dataPageV1.getRlEncoding(),
                          dataPageV1.getDlEncoding(),
                          dataPageV1.getValueEncoding());
                } else {
                  long firstRowIndex = offsetIndex.getFirstRowIndex(currentPageIndex);
                  decompressedPage =
                      new DataPageV1(
                          decompressed,
                          dataPageV1.getValueCount(),
                          dataPageV1.getUncompressedSize(),
                          firstRowIndex,
                          Math.toIntExact(
                              offsetIndex.getLastRowIndex(currentPageIndex, rowCount)
                                  - firstRowIndex
                                  + 1),
                          dataPageV1.getStatistics(),
                          dataPageV1.getRlEncoding(),
                          dataPageV1.getDlEncoding(),
                          dataPageV1.getValueEncoding());
                }
                if (dataPageV1.getCrc().isPresent()) {
                  decompressedPage.setCrc(dataPageV1.getCrc().getAsInt());
                }
                return decompressedPage;
              } catch (IOException e) {
                throw new ParquetDecodingException("could not decompress page", e);
              }
            }

            @Override
            public DataPage visit(DataPageV2 dataPageV2) {
              if (!dataPageV2.isCompressed() && offsetIndex == null && null == blockDecryptor) {
                return dataPageV2;
              }
              BytesInput pageBytes = dataPageV2.getData();

              if (null != blockDecryptor) {
                try {
                  pageBytes =
                      BytesInput.from(blockDecryptor.decrypt(pageBytes.toByteArray(), dataPageAAD));
                } catch (IOException e) {
                  throw new ParquetDecodingException(
                      "could not convert page ByteInput to byte array", e);
                }
              }
              if (dataPageV2.isCompressed()) {
                int uncompressedSize =
                    Math.toIntExact(
                        dataPageV2.getUncompressedSize()
                            - dataPageV2.getDefinitionLevels().size()
                            - dataPageV2.getRepetitionLevels().size());
                try {
                  pageBytes = decompressor.decompress(pageBytes, uncompressedSize);
                } catch (IOException e) {
                  throw new ParquetDecodingException("could not decompress page", e);
                }
              }

              if (offsetIndex == null) {
                return DataPageV2.uncompressed(
                    dataPageV2.getRowCount(),
                    dataPageV2.getNullCount(),
                    dataPageV2.getValueCount(),
                    dataPageV2.getRepetitionLevels(),
                    dataPageV2.getDefinitionLevels(),
                    dataPageV2.getDataEncoding(),
                    pageBytes,
                    dataPageV2.getStatistics());
              } else {
                return DataPageV2.uncompressed(
                    dataPageV2.getRowCount(),
                    dataPageV2.getNullCount(),
                    dataPageV2.getValueCount(),
                    offsetIndex.getFirstRowIndex(currentPageIndex),
                    dataPageV2.getRepetitionLevels(),
                    dataPageV2.getDefinitionLevels(),
                    dataPageV2.getDataEncoding(),
                    pageBytes,
                    dataPageV2.getStatistics());
              }
            }
          });
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = compressedDictionaryPage.getBytes();
        if (null != blockDecryptor) {
          bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
        }
        DictionaryPage decompressedPage =
            new DictionaryPage(
                decompressor.decompress(bytes, compressedDictionaryPage.getUncompressedSize()),
                compressedDictionaryPage.getDictionarySize(),
                compressedDictionaryPage.getEncoding());
        if (compressedDictionaryPage.getCrc().isPresent()) {
          decompressedPage.setCrc(compressedDictionaryPage.getCrc().getAsInt());
        }
        return decompressedPage;
      } catch (IOException e) {
        throw new ParquetDecodingException("Could not decompress dictionary page", e);
      }
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers =
      new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;
  private final long rowIndexOffset;
  private final RowRanges rowRanges;

  public ColumnChunkPageReadStore(long rowCount) {
    this(rowCount, -1);
  }

  ColumnChunkPageReadStore(RowRanges rowRanges) {
    this(rowRanges, -1);
  }

  ColumnChunkPageReadStore(long rowCount, long rowIndexOffset) {
    this.rowCount = rowCount;
    this.rowIndexOffset = rowIndexOffset;
    rowRanges = null;
  }

  ColumnChunkPageReadStore(RowRanges rowRanges, long rowIndexOffset) {
    this.rowRanges = rowRanges;
    this.rowIndexOffset = rowIndexOffset;
    rowCount = rowRanges.rowCount();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Optional<Long> getRowIndexOffset() {
    return rowIndexOffset < 0 ? Optional.empty() : Optional.of(rowIndexOffset);
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    final PageReader pageReader = readers.get(path);
    if (pageReader == null) {
      throw new IllegalArgumentException(
          path + " is not in the store: " + readers.keySet() + " " + rowCount);
    }
    return pageReader;
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    return readers.get(descriptor).readDictionaryPage();
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
  }

  void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new RuntimeException(path + " was added twice");
    }
  }
}

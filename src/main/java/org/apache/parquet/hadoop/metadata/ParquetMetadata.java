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
package org.apache.parquet.hadoop.metadata;

import java.util.List;


/**
 * Meta Data block stored in the footer of the file
 * contains file level (Codec, Schema, ...) and block level (location, columns, record count, ...) meta data
 */
public class ParquetMetadata {
  private final FileMetaData fileMetaData;
  private final List<BlockMetaData> blocks;

  /**
   * @param fileMetaData file level metadata
   * @param blocks       block level metadata
   */
  public ParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks) {
    this.fileMetaData = fileMetaData;
    this.blocks = blocks;
  }

  /**
   * @return block level metadata
   */
  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  /**
   * @return file level meta data
   */
  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }


  @Override
  public String toString() {
    return "ParquetMetaData{fileMetaData: " + fileMetaData + ", blocks: " + blocks + "}";
  }

}

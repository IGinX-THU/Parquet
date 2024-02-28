package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import cn.edu.tsinghua.iginx.format.parquet.test.FileSystemUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.column.Encoding.*;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExampleParquetWriterTest {

  @Test
  public void test() throws Exception {
    Path root = Files.createTempDirectory(null);
    FileSystemUtils.enforceEmptyDir(root);
    MessageType schema = parseMessageType("message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");

    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    Map<String, Encoding> expected = new HashMap<>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
    for (int modulo : Arrays.asList(10, 1000)) {
      for (ParquetProperties.WriterVersion version : ParquetProperties.WriterVersion.values()) {
        Path file = root.resolve(version.name() + "_" + modulo + ".parquet");

        // write
        try (TrackingByteBufferAllocator allocator = new TrackingByteBufferAllocator(new HeapByteBufferAllocator())) {
          ExampleParquetWriter.Builder writerBuilder = ExampleParquetWriter.builder(file, schema)
              .withAllocator(new HeapByteBufferAllocator())
              .withCompressionCodec(UNCOMPRESSED)
              .withRowGroupSize(1024)
              .withPageSize(1024)
              .withDictionaryPageSize(512)
              .withDictionaryEncoding(true)
              .withValidation(false)
              .withWriterVersion(version);
          try (ParquetWriter<Group> writer = writerBuilder.build()) {
            for (int i = 0; i < 1000; i++) {
              writer.write(f.newGroup()
                  .append("binary_field", "test" + (i % modulo))
                  .append("int32_field", 32)
                  .append("int64_field", 64L)
                  .append("boolean_field", true)
                  .append("float_field", 1.0f)
                  .append("double_field", 2.0d)
                  .append("flba_field", "foo")
                  .append("int96_field", Binary.fromConstantByteArray(new byte[12])));
            }
          }
        }

        // read
        try (TrackingByteBufferAllocator allocator = new TrackingByteBufferAllocator(new HeapByteBufferAllocator())) {
          ExampleParquetReader.Builder readerBuilder = ExampleParquetReader.builder(file)
              .withAllocator(new HeapByteBufferAllocator());
          try (ExampleParquetReader reader = readerBuilder.build()) {
            for (int i = 0; i < 1000; i++) {
              Group group = reader.read();
              assertEquals(
                  "test" + (i % modulo),
                  group.getBinary("binary_field", 0).toStringUsingUTF8());
              assertEquals(32, group.getInteger("int32_field", 0));
              assertEquals(64L, group.getLong("int64_field", 0));
              assertTrue(group.getBoolean("boolean_field", 0));
              assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
              assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
              assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
              assertEquals(Binary.fromConstantByteArray(new byte[12]), group.getInt96("int96_field", 0));
            }

            ParquetMetadata footer = reader.getFooter();
            for (BlockMetaData blockMetaData : footer.getBlocks()) {
              for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
                if (column.getPath().toDotString().equals("binary_field")) {
                  String key = modulo + "-" + version;
                  Encoding expectedEncoding = expected.get(key);
                  assertTrue(
                      column.getEncodings().contains(expectedEncoding),
                      key + ":" + column.getEncodings() + " should contain " + expectedEncoding);
                }
              }
            }
            assertEquals(
                ExampleParquetWriter.OBJECT_MODEL_NAME_VALUE,
                footer.getFileMetaData().getKeyValueMetaData().get(ParquetWriter.OBJECT_MODEL_NAME_PROP),
                "Object model property should be example");
          }
        }

        Files.delete(file);
      }
    }
  }
}
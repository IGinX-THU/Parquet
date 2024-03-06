package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import shaded.iginx.org.apache.parquet.bytes.HeapByteBufferAllocator;
import shaded.iginx.org.apache.parquet.example.data.Group;
import shaded.iginx.org.apache.parquet.example.data.simple.SimpleGroupFactory;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.io.api.Binary;
import shaded.iginx.org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static shaded.iginx.org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static shaded.iginx.org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static shaded.iginx.org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class WriteReadExample {
  public static void main(String[] args) throws IOException {
    Path file = Files.createTempFile(null, ".parquet");

    // prepare a schema
    MessageType schema = parseMessageType("message test { " + "required binary binary_field; " + "required int32 int32_field; " + "required int64 int64_field; " + "required boolean boolean_field; " + "required float float_field; " + "required double double_field; " + "required fixed_len_byte_array(3) flba_field; " + "required int96 int96_field; " + "} ");

    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    // build a writer
    ExampleParquetWriter.Builder writerBuilder = ExampleParquetWriter.builder(file, schema)
        .withAllocator(new HeapByteBufferAllocator())
        .withCodec(UNCOMPRESSED) // Set the compression codec
        .withRowGroupSize(1024)
        .withPageSize(1024)
        .withDictionaryPageSize(512)
        .withDictionaryEncoding(true)
        .withValidation(false)
        .withWriterVersion(PARQUET_1_0)
        .withOverwrite(true);

    // write records
    try (ParquetWriter<Group> writer = writerBuilder.build()) {
      for (int i = 0; i < 1000; i++) {
        writer.write(f.newGroup().append("binary_field", "test").append("int32_field", 32).append("int64_field", 64L).append("boolean_field", true).append("float_field", 1.0f).append("double_field", 2.0d).append("flba_field", "foo").append("int96_field", Binary.fromConstantByteArray(new byte[12])));
      }
    }

    // build a reader
    ExampleParquetReader.Builder readerBuilder = ExampleParquetReader.builder(file)
        .withAllocator(new HeapByteBufferAllocator());

    // read records
    try (ExampleParquetReader reader = readerBuilder.build()) {
      for (int i = 0; i < 1000; i++) {
        Group group = reader.read();
        assert "test".equals(group.getBinary("binary_field", 0).toStringUsingUTF8());
        assert 32 == group.getInteger("int32_field", 0);
        assert 64L == group.getLong("int64_field", 0);
        assert group.getBoolean("boolean_field", 0);
        assert 1.0f == group.getFloat("float_field", 0);
        assert 2.0d == group.getDouble("double_field", 0);
        assert "foo".equals(group.getBinary("flba_field", 0).toStringUsingUTF8());
        assert Binary.fromConstantByteArray(new byte[12]).equals(group.getInt96("int96_field", 0));
      }

      // read the footer
      ParquetMetadata footer = reader.getFooter();
      assert ExampleParquetWriter.OBJECT_MODEL_NAME_VALUE.equals(footer.getFileMetaData().getKeyValueMetaData().get(ParquetWriter.OBJECT_MODEL_NAME_PROP));
      System.out.println(ParquetMetadata.toPrettyJSON(footer));
    }

    Files.deleteIfExists(file);
  }
}

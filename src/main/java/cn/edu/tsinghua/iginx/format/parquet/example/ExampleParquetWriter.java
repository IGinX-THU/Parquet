package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.example.GroupDematerializer;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordDematerializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ExampleParquetWriter extends ParquetWriter<Group> {
  public static final String OBJECT_MODEL_NAME_VALUE = "example";

  protected ExampleParquetWriter(ParquetRecordWriter<Group> recordWriter) throws IOException {
    super(recordWriter);
  }

  public static Builder builder(Path file, MessageType schema) {
    return new Builder(new LocalOutputFile(file, new HeapByteBufferAllocator(), Integer.MAX_VALUE), schema);
  }

  public static class Builder extends ParquetWriter.Builder<Group, ExampleParquetWriter, Builder> {
    private final OutputFile file;
    private final MessageType schema;

    public Builder(LocalOutputFile file, MessageType schema) {
      this.file = file;
      this.schema = schema;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected RecordDematerializer<Group> getDematerializer() {
      Map<String, String> extraMetaData = new HashMap<>();
      extraMetaData.put(OBJECT_MODEL_NAME_PROP, OBJECT_MODEL_NAME_VALUE);
      return new GroupDematerializer(schema, extraMetaData);
    }

    @Override
    public ExampleParquetWriter build() throws IOException {
      ParquetRecordWriter<Group> recordWriter = build(file);
      return new ExampleParquetWriter(recordWriter);
    }
  }
}

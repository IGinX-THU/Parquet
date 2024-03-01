package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetReader;
import cn.edu.tsinghua.iginx.format.parquet.io.LocalInputFile;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ExportedParquetRecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ExampleParquetReader extends ParquetReader<Group> {
  private final ExportedParquetRecordReader<Group> recordReader;

  protected ExampleParquetReader(ExportedParquetRecordReader<Group> recordReader) {
    super(recordReader);
    this.recordReader = recordReader;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public ParquetMetadata getFooter() {
    return recordReader.getReader().getFooter();
  }

  public static class Builder extends ParquetReader.Builder<Group, ExampleParquetReader, Builder> {
    private final InputFile file;

    private ParquetMetadata metadata;

    public Builder(InputFile file) {
      this.file = file;
    }

    private ParquetMetadata getMetadata() throws IOException {
      if (metadata == null) {
        metadata = readFooter(file);
      }
      return metadata;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected RecordMaterializer<Group> materializer(MessageType schema, Map<String, String> extra) throws IOException {
      return new GroupRecordConverter(schema);
    }

    @Override
    public ExampleParquetReader build() throws IOException {
      return new ExampleParquetReader(build(file, getMetadata()));
    }
  }
}

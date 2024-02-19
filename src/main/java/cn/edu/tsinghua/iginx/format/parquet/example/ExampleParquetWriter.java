package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetWriteOptions;
import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class ExampleParquetWriter extends ParquetWriter<Group> {
  public ExampleParquetWriter(OutputFile file, MessageType schema, ParquetWriteOptions options) throws IOException {
    super(file, new GroupDematerializer(schema), options);
  }
}

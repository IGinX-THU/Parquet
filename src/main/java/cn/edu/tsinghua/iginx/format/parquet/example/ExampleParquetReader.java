package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetReadOptions;
import cn.edu.tsinghua.iginx.format.parquet.ParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

public class ExampleParquetReader extends ParquetReader<Group> {
  public ExampleParquetReader(InputFile file, ParquetReadOptions options) throws IOException {
    super(file, GroupRecordConverter::new, options);
  }
}

package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.api.RecordDematerializer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Objects;

public class GroupDematerializer extends RecordDematerializer<Group> {
  private final MessageType schema;
  private GroupWriter groupWriter;

  public GroupDematerializer(MessageType schema) {
    Objects.requireNonNull(schema);
    this.schema = schema;
  }

  @Override
  public void prepare(RecordConsumer recordConsumer) {
    groupWriter = new GroupWriter(recordConsumer, schema);
  }

  @Override
  public void write(Group record) {
    if (groupWriter != null) {
      groupWriter.write(record);
    }
  }

}

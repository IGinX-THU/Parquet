package org.apache.parquet.hadoop.example;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordDematerializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Objects;

public class GroupDematerializer extends RecordDematerializer<Group> {
  private final MessageType schema;
  private final Map<String, String> extraMetaData;

  private GroupWriter groupWriter;

  public GroupDematerializer(MessageType schema, Map<String, String> extraMetaData) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(extraMetaData);
    this.schema = schema;
    this.extraMetaData = extraMetaData;
  }

  @Override
  public void setRecordConsumer(RecordConsumer recordConsumer) {
    groupWriter = new GroupWriter(recordConsumer, schema);
  }

  @Override
  public void write(Group record) {
    if (groupWriter != null) {
      groupWriter.write(record);
    }
  }

  @Override
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public Map<String, String> getExtraMetaData() {
    return extraMetaData;
  }
}

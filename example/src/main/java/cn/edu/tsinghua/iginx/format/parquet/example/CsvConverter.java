package cn.edu.tsinghua.iginx.format.parquet.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import shaded.iginx.org.apache.parquet.example.data.Group;
import shaded.iginx.org.apache.parquet.example.data.simple.SimpleGroupFactory;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvConverter {

  public static void main(String[] args) throws IOException {

    // read data from CSV
    String oldCsvContent;
    try (InputStream in = CsvConverter.class.getClassLoader().getResourceAsStream("example.csv")) {
      assert in != null;
      oldCsvContent = new BufferedReader(new InputStreamReader(in))
          .lines().collect(Collectors.joining(System.lineSeparator()));
    }

    // prepare a temp file
    File tempFile = File.createTempFile("example", ".parquet");
    Files.deleteIfExists(tempFile.toPath());
    tempFile.deleteOnExit();

    // prepare a temp string builder
    StringBuilder sb = new StringBuilder();

    // convert from CSV to Parquet
    fromCsvToParquet(new ByteArrayInputStream(oldCsvContent.getBytes()), tempFile.toPath());

    // convert from Parquet to CSV
    fromParquetToCsv(tempFile.toPath(), sb);

    // compare the original CSV and the CSV converted from Parquet
    assert oldCsvContent.contentEquals(sb);
    System.out.println("the original CSV and the CSV converted from Parquet are the same");
  }

  public static void fromCsvToParquet(InputStream csvContent, Path parquetPath) throws IOException {
    // read data from CSV
    Map.Entry<MessageType, List<Group>> dataFromCsv = fromCsv(csvContent);

    System.out.println("Data from CSV:");
    System.out.println(dataFromCsv.getKey());
    for (Group group : dataFromCsv.getValue()) {
      System.out.println(group);
    }

    // write data to Parquet
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(parquetPath, dataFromCsv.getKey());
    try (ExampleParquetWriter writer = builder.build()) {
      for (Group group : dataFromCsv.getValue()) {
        writer.write(group);
      }
    }
  }

  public static void fromParquetToCsv(Path parquetPath, Appendable dist) throws IOException {
    // read data from Parquet
    List<List<String>> records = fromParquet(parquetPath);

    System.out.println("Data from Parquet:");
    for (List<String> record : records) {
      System.out.println(record);
    }

    // write data to CSV
    CSVFormat.DEFAULT.print(dist).printRecords(records);
  }

  public static MessageType getSchema(String tableName, List<String> header, List<CsvType> types) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    if (header.size() != types.size()) {
      throw new IllegalArgumentException("The number of columns in the header and types are not consistent");
    }

    for (int i = 0; i < header.size(); i++) {
      String name = header.get(i);
      switch (types.get(i)) {
        case BOOLEAN:
          builder.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
          break;
        case INT:
          builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(name);
          break;
        case FLOAT:
          builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
          break;
        case STRING:
          builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
          break;
        default:
          throw new IllegalArgumentException("Unknown type");
      }
    }

    return builder.named(tableName);
  }

  public static Map.Entry<MessageType, List<Group>> fromCsv(InputStream csv) throws IOException {
    List<Group> groups = new ArrayList<>();

    Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new InputStreamReader(csv));
    List<String> header = null;
    List<CsvType> types = null;
    MessageType schema = null;
    SimpleGroupFactory groupFactory = null;
    for (CSVRecord record : records) {

      // set header
      if (header == null) {
        header = record.toList();
        continue;
      }

      if (record.size() != header.size()) {
        throw new IOException("The number of columns in the csv file is not consistent");
      }

      // detect types
      if (types == null) {
        types = new ArrayList<>();
        for (int i = 0; i < record.size(); i++) {
          types.add(CsvType.detect(record.get(i)));
        }
        schema = getSchema("csv-example", header, types);
        groupFactory = new SimpleGroupFactory(schema);
      }

      // convert to parquet example group
      Group group = groupFactory.newGroup();
      for (int i = 0; i < record.size(); i++) {
        String value = record.get(i);
        switch (types.get(i)) {
          case BOOLEAN:
            group.append(header.get(i), Boolean.parseBoolean(value));
            break;
          case INT:
            group.append(header.get(i), Long.parseLong(value));
            break;
          case FLOAT:
            group.append(header.get(i), Double.parseDouble(value));
            break;
          case STRING:
            group.append(header.get(i), value);
            break;
          default:
            throw new IllegalArgumentException("Unknown type");
        }
      }

      groups.add(group);
    }

    return new AbstractMap.SimpleImmutableEntry<>(schema, groups);
  }

  public static List<List<String>> fromParquet(Path parquetPath) throws IOException {
    ExampleParquetReader.Builder builder = ExampleParquetReader.builder(parquetPath);

    List<List<String>> records = new ArrayList<>();
    try (ExampleParquetReader reader = builder.build()) {
      List<String> header = new ArrayList<>();
      ParquetMetadata metadata = reader.getFooter();
      MessageType schema = metadata.getFileMetaData().getSchema();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        header.add(schema.getType(i).getName());
      }
      records.add(header);

      Group group;
      while ((group = reader.read()) != null) {
        List<String> record = new ArrayList<>();
        for (int i = 0; i < group.getType().getFieldCount(); i++) {
          Type type = group.getType().getType(i);
          if (!type.isPrimitive()) {
            throw new IllegalArgumentException("The type of the field is not primitive");
          }
          if (type.asPrimitiveType().isRepetition(Type.Repetition.REPEATED)) {
            throw new IllegalArgumentException("The type of the field is repeated");
          }
          record.add(group.getValueToString(i, 0));
        }
        records.add(record);
      }
    }
    return records;
  }

  enum CsvType {
    BOOLEAN,
    INT,
    FLOAT,
    STRING;

    public static CsvType detect(String value) {
      value = value.toLowerCase();
      if (value.equals("true") || value.equals("false")) {
        return BOOLEAN;
      } else {
        try {
          Long.parseLong(value);
          return INT;
        } catch (NumberFormatException e) {
          try {
            Double.parseDouble(value);
            return FLOAT;
          } catch (NumberFormatException e2) {
            return STRING;
          }
        }
      }
    }
  }
}

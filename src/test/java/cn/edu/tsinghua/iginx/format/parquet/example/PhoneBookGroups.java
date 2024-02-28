package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetReader;
import cn.edu.tsinghua.iginx.format.parquet.ParquetWriter;
import cn.edu.tsinghua.iginx.format.parquet.test.PhoneBook;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PhoneBookGroups {

  public static Group from(PhoneBook.User user) {
    SimpleGroup root = new SimpleGroup(PhoneBook.SCHEMA);
    root.append("id", user.getId());

    if (user.getName() != null) {
      root.append("name", user.getName());
    }

    if (user.getLocation() != null) {
      Group location = root.addGroup("location");
      if (user.getLocation().getLon() != null) {
        location.append("lon", user.getLocation().getLon());
      }
      if (user.getLocation().getLat() != null) {
        location.append("lat", user.getLocation().getLat());
      }
    }

    if (user.getPhoneNumbers() != null) {
      Group phoneNumbers = root.addGroup("phoneNumbers");
      for (PhoneBook.PhoneNumber number : user.getPhoneNumbers()) {
        Group phone = phoneNumbers.addGroup("phone");
        phone.append("number", number.getNumber());
        if (number.getKind() != null) {
          phone.append("kind", number.getKind());
        }
      }
    }
    return root;
  }

  public static PhoneBook.User parseUser(Group user) {
    if (user == null) {
      return null;
    }
    return new PhoneBook.User(
        extractRequiredLong(user, "id"),
        extractString(user, "name"),
        parsePhoneNumbers(extractGroup(user, "phoneNumbers")),
        parseLocation(extractGroup(user, "location")));
  }

  public static List<PhoneBook.PhoneNumber> parsePhoneNumbers(Group phoneNumbers) {
    if (phoneNumbers == null) {
      return null;
    }
    List<PhoneBook.PhoneNumber> list = new ArrayList<>();
    for (int i = 0, n = phoneNumbers.getFieldRepetitionCount("phone"); i < n; ++i) {
      Group phone = phoneNumbers.getGroup("phone", i);
      list.add(new PhoneBook.PhoneNumber(extractRequiredLong(phone, "number"), extractString(phone, "kind")));
    }
    return list;
  }

  public static PhoneBook.Location parseLocation(Group location) {
    if (location == null) {
      return null;
    }
    return new PhoneBook.Location(extractDouble(location, "lon"), extractDouble(location, "lat"));
  }

  private static boolean isNull(Group group, String field) {
    // Use null value if the field is not in the group schema
    if (!group.getType().containsField(field)) {
      return true;
    }
    int repetition = group.getFieldRepetitionCount(field);
    if (repetition == 0) {
      return true;
    } else if (repetition == 1) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid repetitionCount " + repetition + " for field " + field + " in group " + group);
  }

  private static long extractRequiredLong(Group group, String field) {
    if (isNull(group, field)) {
      throw new IllegalArgumentException("Field " + field + " is required, but it was not found in " + group);
    }
    return group.getLong(field, 0);
  }

  private static String extractString(Group group, String field) {
    return isNull(group, field) ? null : group.getString(field, 0);
  }

  private static Double extractDouble(Group group, String field) {
    return isNull(group, field) ? null : group.getDouble(field, 0);
  }

  private static Group extractGroup(Group group, String field) {
    return isNull(group, field) ? null : group.getGroup(field, 0);
  }

  public static void write(ExampleParquetWriter.Builder writerBuilder, List<Group> groups) throws IOException {
    try (ParquetWriter<Group> writer = writerBuilder.build()) {
      for (Group group : groups) {
        writer.write(group);
      }
    }
  }

  public static void writeUsers(ExampleParquetWriter.Builder writerBuilder, List<PhoneBook.User> users) throws IOException {
    List<Group> groups = users.stream().map(PhoneBookGroups::from).collect(Collectors.toList());
    write(writerBuilder, groups);
  }

  public static List<PhoneBook.User> readUsers(ExampleParquetReader.Builder readerBuilder) throws IOException {
    List<Map.Entry<Long, PhoneBook.User>> usersWithRowIndex = readUsersWithRowIndex(readerBuilder);
    return usersWithRowIndex.stream().map(Map.Entry::getValue).collect(Collectors.toList());
  }

  public static List<Map.Entry<Long, PhoneBook.User>> readUsersWithRowIndex(ExampleParquetReader.Builder readerBuilder) throws IOException {
    try (ParquetReader<Group> reader = readerBuilder.build()) {
      List<Map.Entry<Long, PhoneBook.User>> users = new ArrayList<>();
      while (true) {
        Group group = reader.read();
        if (group == null) {
          break;
        }
        PhoneBook.User user = parseUser(group);
        long rowIndex = reader.getCurrentRowIndex();
        users.add(new AbstractMap.SimpleEntry<>(rowIndex, user));
      }
      return users;
    }
  }

  public static ParquetReader<Group> createReader(Path file, FilterCompat.Filter filter, ByteBufferAllocator allocator)
      throws IOException {
    return ExampleParquetReader.builder(file).withFilter(filter).withAllocator(allocator).build();
  }

  public static List<Map.Entry<Long, Group>> readFrom(Path path, FilterCompat.Filter filter) throws IOException {
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
         ParquetReader<Group> reader = createReader(path, filter, allocator)) {

      List<Map.Entry<Long, Group>> users = new ArrayList<>();
      while (true) {
        Group group = reader.read();
        if (group == null) {
          break;
        }
        long rowIndex = reader.getCurrentRowIndex();
        users.add(new AbstractMap.SimpleEntry<>(rowIndex, group));
      }
      return users;
    }
  }

  public static List<Map.Entry<Long, PhoneBook.User>> readUsersFrom(Path path, FilterCompat.Filter filter) throws IOException {
    List<Map.Entry<Long, Group>> groups = readFrom(path, filter);
    return groups.stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), parseUser(e.getValue()))).collect(Collectors.toList());
  }
}

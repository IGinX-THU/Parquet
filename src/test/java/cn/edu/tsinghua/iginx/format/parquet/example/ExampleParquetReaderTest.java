package cn.edu.tsinghua.iginx.format.parquet.example;

import cn.edu.tsinghua.iginx.format.parquet.ParquetReader;
import cn.edu.tsinghua.iginx.format.parquet.test.PhoneBook;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExampleParquetReaderTest {
  private static final Path FILE_V1;
  private static final Path FILE_V2;
  private static final List<PhoneBook.User> DATA = Collections.unmodifiableList(PhoneBook.makeUsers(1000));

  static {
    try {
      FILE_V1 = Files.createTempFile("File_v1.", ".parquet");
      FILE_V2 = Files.createTempFile("File_v2.", ".parquet");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<? extends Arguments> provideArguments() {
    return Stream.of(
        Arguments.of(FILE_V1),
        Arguments.of(FILE_V2)
    );
  }


  @BeforeAll
  public static void createFiles() throws IOException {
    writePhoneBookToFile(FILE_V1, ParquetProperties.WriterVersion.PARQUET_1_0);
    writePhoneBookToFile(FILE_V2, ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  @AfterAll
  public static void deleteFiles() throws IOException {
    Files.deleteIfExists(FILE_V1);
    Files.deleteIfExists(FILE_V2);
  }

  private static void writePhoneBookToFile(Path file, ParquetProperties.WriterVersion parquetVersion)
      throws IOException {
    int pageSize = DATA.size() / 10; // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

    PhoneBookGroups.writeUsers(
        ExampleParquetWriter.builder(file, PhoneBook.SCHEMA)
            .withOverwrite(true)
            .withRowGroupSize(rowGroupSize)
            .withPageSize(pageSize)
            .withWriterVersion(parquetVersion),
        DATA);
  }

  private static List<Map.Entry<Long, PhoneBook.User>> readUsersWithRowIndex(
      Path file,
      FilterCompat.Filter filter,
      boolean useOtherFiltering,
      boolean useColumnIndexFilter,
      long rangeStart,
      long rangeEnd)
      throws IOException {
    try (TrackingByteBufferAllocator allocator = new TrackingByteBufferAllocator(new HeapByteBufferAllocator())) {
      return PhoneBookGroups.readUsersWithRowIndex(
          ExampleParquetReader.builder(file)
              .withAllocator(new HeapByteBufferAllocator())
              .withFilter(filter)
              .useDictionaryFilter(useOtherFiltering)
              .useStatsFilter(useOtherFiltering)
              .useRecordFilter(useOtherFiltering)
              .useColumnIndexFilter(useColumnIndexFilter)
              .withFileRange(rangeStart, rangeEnd));
    }
  }

  private static List<PhoneBook.User> readUsers(
      Path file,
      FilterCompat.Filter filter,
      boolean useOtherFiltering,
      boolean useColumnIndexFilter)
      throws IOException {
    List<Map.Entry<Long, PhoneBook.User>> usersWithRowIndex = readUsersWithRowIndex(file, filter, useOtherFiltering, useColumnIndexFilter, 0, Long.MAX_VALUE);
    return usersWithRowIndex.stream().map(Map.Entry::getValue).collect(Collectors.toList());
  }

  public static void validateRowIndexes(List<Map.Entry<Long, PhoneBook.User>> users) {
    for (Map.Entry<Long, PhoneBook.User> entry : users) {
      PhoneBook.User u = entry.getValue();
      long rowIndex = entry.getKey();
      assertEquals(u.getId(), rowIndex, "Row index should be same as user id");
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testCurrentRowIndex(Path path) throws Exception {
    try (TrackingByteBufferAllocator allocator = new TrackingByteBufferAllocator(new HeapByteBufferAllocator());
         ParquetReader<Group> reader = ExampleParquetReader.builder(path).withAllocator(new HeapByteBufferAllocator()).build()) {
      // Fetch row index without processing any row.
      assertEquals(-1, reader.getCurrentRowIndex());
      reader.read();
      assertEquals(0, reader.getCurrentRowIndex());
      // calling the same API again and again should return same result.
      assertEquals(0, reader.getCurrentRowIndex());

      reader.read();
      assertEquals(1, reader.getCurrentRowIndex());
      assertEquals(1, reader.getCurrentRowIndex());
      long expectedCurrentRowIndex = 2L;
      while (reader.read() != null) {
        assertEquals(expectedCurrentRowIndex, reader.getCurrentRowIndex());
        expectedCurrentRowIndex++;
      }
      // reader.read() returned null and so reader doesn't have any more rows.
      assertEquals(-1, reader.getCurrentRowIndex());
    }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testRangeFiltering(Path path) throws Exception {
    long fileSize = Files.size(path);
    // The readUsers also validates the rowIndex for each returned row.
    validateRowIndexes(readUsersWithRowIndex(path, FilterCompat.NOOP, false, false, fileSize / 2, fileSize));
    validateRowIndexes(readUsersWithRowIndex(path, FilterCompat.NOOP, true, false, fileSize / 3, fileSize * 3 / 4));
    validateRowIndexes(readUsersWithRowIndex(path, FilterCompat.NOOP, false, true, fileSize / 4, fileSize / 2));
    validateRowIndexes(readUsersWithRowIndex(path, FilterCompat.NOOP, true, true, fileSize * 3 / 4, fileSize));
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testSimpleFiltering(Path path) throws Exception {
    Set<Long> idSet = new HashSet<>();
    idSet.add(123L);
    idSet.add(567L);
    // The readUsers also validates the rowIndex for each returned row.
    List<PhoneBook.User> filteredUsers1 =
        readUsers(path, FilterCompat.get(in(longColumn("id"), idSet)), true, true);
    assertEquals(2L, filteredUsers1.size());
    List<PhoneBook.User> filteredUsers2 =
        readUsers(path, FilterCompat.get(in(longColumn("id"), idSet)), true, false);
    assertEquals(2L, filteredUsers2.size());
    List<PhoneBook.User> filteredUsers3 =
        readUsers(path, FilterCompat.get(in(longColumn("id"), idSet)), false, false);
    assertEquals(1000L, filteredUsers3.size());
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  public void testNoFiltering(Path path) throws Exception {
    assertEquals(DATA, readUsers(path, FilterCompat.NOOP, false, false));
    assertEquals(DATA, readUsers(path, FilterCompat.NOOP, true, false));
    assertEquals(DATA, readUsers(path, FilterCompat.NOOP, false, true));
    assertEquals(DATA, readUsers(path, FilterCompat.NOOP, true, true));
  }

}
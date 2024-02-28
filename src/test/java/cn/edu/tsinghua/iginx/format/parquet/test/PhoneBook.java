package cn.edu.tsinghua.iginx.format.parquet.test;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.*;

public class PhoneBook {

  private static final String schemaString = "message user {\n"
      + "  required int64 id;\n"
      + "  optional binary name (UTF8);\n"
      + "  optional group location {\n"
      + "    optional double lon;\n"
      + "    optional double lat;\n"
      + "  }\n"
      + "  optional group phoneNumbers {\n"
      + "    repeated group phone {\n"
      + "      required int64 number;\n"
      + "      optional binary kind (UTF8);\n"
      + "    }\n"
      + "  }\n"
      + "}\n";

  public static final MessageType SCHEMA = MessageTypeParser.parseMessageType(schemaString);

  public static List<User> makeUsers(int rowCount) {
    List<User> users = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Location location = null;
      if (i % 3 == 1) {
        location = new Location((double) i, (double) i * 2);
      }
      if (i % 3 == 2) {
        location = new Location((double) i, null);
      }
      // row index of each row in the file is same as the user id.
      users.add(new User(
          i, "p" + i, Collections.singletonList(new PhoneNumber(i, "cell")), location));
    }
    return users;
  }

  public static class Location {
    private final Double lon;
    private final Double lat;

    public Location(Double lon, Double lat) {
      this.lon = lon;
      this.lat = lat;
    }

    public Double getLon() {
      return lon;
    }

    public Double getLat() {
      return lat;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Location location = (Location) o;
      return Objects.equals(lon, location.lon) && Objects.equals(lat, location.lat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lon, lat);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Location.class.getSimpleName() + "[", "]")
          .add("lon=" + lon)
          .add("lat=" + lat)
          .toString();
    }
  }

  public static class PhoneNumber {
    private final long number;
    private final String kind;

    public PhoneNumber(long number, String kind) {
      this.number = number;
      this.kind = kind;
    }

    public long getNumber() {
      return number;
    }

    public String getKind() {
      return kind;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PhoneNumber that = (PhoneNumber) o;
      return number == that.number && Objects.equals(kind, that.kind);
    }

    @Override
    public int hashCode() {
      return Objects.hash(number, kind);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", PhoneNumber.class.getSimpleName() + "[", "]")
          .add("number=" + number)
          .add("kind='" + kind + "'")
          .toString();
    }
  }

  public static class User {
    private final long id;
    private final String name;
    private final Location location;
    private final List<PhoneNumber> phoneNumbers;

    public User(long id, String name, List<PhoneNumber> phoneNumbers, Location location) {
      this.id = id;
      this.name = name;
      this.phoneNumbers = phoneNumbers;
      this.location = location;
    }

    public long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public Location getLocation() {
      return location;
    }

    public List<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      User user = (User) o;
      return id == user.id && Objects.equals(name, user.name) && Objects.equals(location, user.location) && Objects.equals(phoneNumbers, user.phoneNumbers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, location, phoneNumbers);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", User.class.getSimpleName() + "[", "]")
          .add("id=" + id)
          .add("name='" + name + "'")
          .add("location=" + location)
          .add("phoneNumbers=" + phoneNumbers)
          .toString();
    }

    public User cloneWithName(String name) {
      return new User(id, name, phoneNumbers, location);
    }
  }
}

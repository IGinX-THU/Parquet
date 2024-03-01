package org.apache.hadoop.fs;

import java.util.StringJoiner;

public class Path {

  private final String path;

  public Path(String string) {
    this.path = string;
  }


  @Override
  public String toString() {
    return new StringJoiner(", ", Path.class.getSimpleName() + "[", "]")
        .add("path='" + path + "'")
        .toString();
  }
}

package org.apache.hadoop.conf;

public class Configuration {
  public boolean getBoolean(String strictTypeChecking, boolean defaultValue) {
    return defaultValue;
  }

  public void set(String property, String value) {
  }

  public int getInt(String parquetReadParallelism, int defaultValue) {
    return defaultValue;
  }
}
package cn.edu.tsinghua.iginx.format.parquet.test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileSystemUtils {
  public static void enforceEmptyDir(Path path) throws IOException {
    if (Files.exists(path)) {
      Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
          Files.delete(path);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          if (exc != null) {
            throw exc;
          }
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
    Files.createDirectories(path);
  }
}

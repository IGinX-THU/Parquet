# Parquet-IGinX

This is a fork of Parquet-MR which contains the java implementation of the Parquet format. This fork is used by IGinX to
support the Parquet format.

## Usage

To use this library, add the following dependency to your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>cn.edu.tsinghua.iginx</groupId>
        <artifactId>parquet-file</artifactId>
        <version>0.0.3</version>
    </dependency>
</dependencies>
```

and replace `{parquet-file.version}` with the version of the library you want to use.

You also need to add the following repository to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>parquet-file</id>
        <name>IGinX GitHub repository</name>
        <url>https://iginx-thu.github.io/Parquet/maven-repo</url>
    </repository>
</repositories>
```

You can find the example usage of this library in the `example` directory.

Please see test cases in `src/test` for more examples.

## Building from source

To build the library from source, run the following command:

```bash
mvn clean install
```

## Contributing

Contributions are welcome. Please open a pull request or an issue if you would like to contribute.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

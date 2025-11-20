# CSV Connector

CSV connector implementation for the Universal Data Connectors (UDC) platform.

It plugs into the shared `connector-spi` module and can be discovered through Java's `ServiceLoader` using the service definition in `src/main/resources/META-INF/services/io.github.dataconnector.spi.DataConnector`.

## Features

- Reads CSV content from the filesystem, classpath resources, or in-memory strings/byte arrays supplied via the connector context.
- Flexible parsing controls: custom delimiter/quote characters, optional header detection, trimming, comment skipping, selective row starts, charset control, and optional skipping of empty lines.
- Rich diagnostics: descriptive success/failure `ConnectorResult` responses and SLF4J-based logging.
- `write` operation is intentionally left unimplemented for now (throws `UnsupportedOperationException`).

## Requirements

- JDK 17 or newer.
- Maven 3.9+.
- Access to the `connector-parent` and `connector-spi` artifacts (published or locally installed).

## Build

```bash
mvn clean package
```

The resulting artifact is `target/connector-csv-0.0.1.jar`.

## Usage

1. Add the connector JAR (and its dependencies) to your runtime classpath.
2. Ensure the SPI service file remains on the classpath so the connector is discoverable.
3. Build a `ConnectorContext` with the desired configuration map, then call `read(context)` on the connector instance (manually or via the SPI facade).

### Minimal example

```java
CsvConnector connector = new CsvConnector();
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "file_path", "/data/customers.csv",
        "delimiter", ";",
        "charset", "UTF-8",
        "use_first_row_as_header", true
    ))
    .build();

ConnectorResult result = connector.read(context);
if (result.isSuccess()) {
    result.getRecords().forEach(System.out::println);
} else {
    System.err.println(result.getMessage());
}
```

### In-memory data

```java
String csv = "id,name\n1,Alice\n2,Bob\n";
ConnectorContext context = ConnectorContext.builder()
    .configuration(Map.of(
        "input_data", csv,
        "skip_empty_rows", true
    ))
    .build();
ConnectorResult result = connector.read(context);
```

## Configuration Options

| Key                        | Type      | Default | Description |
|----------------------------|-----------|---------|-------------|
| `file_path`                | `String`  | —       | Path to local file or classpath resource name. Optional if `input_data` is supplied. |
| `input_data`               | `String` / `byte[]` | — | Raw CSV payload provided directly. |
| `delimiter`                | `String` (1 char) | `,` | Column separator. |
| `quote_char`               | `String` (1 char) | `"` | Quote character. |
| `use_first_row_as_header`  | `Boolean` | `true`  | Treat the first row as headers. |
| `skip_empty_rows`          | `Boolean` | `true`  | Skip blank lines. |
| `trim_spaces`              | `Boolean` | `false` | Trim surrounding spaces of values. |
| `start_row`                | `Integer` | `0`     | Number of rows to skip before parsing. |
| `charset`                  | `String`  | `UTF-8` | Charset name used when decoding strings/streams. |

At least one of `file_path` or `input_data` must be set; otherwise `validateConfiguration` and `read` return an error.

## Testing & Validation

- `mvn -q test` (tests live in the parent project; this module currently has no dedicated unit tests).
- Manual verification: run the minimal example or wire the connector into the UDC runtime and verify `ConnectorResult` output.

## Contributing

1. Fork/clone the repository.
2. Create a feature branch.
3. Add tests or manual verification steps for your change.
4. Open a PR describing the behavior change and configuration impacts.

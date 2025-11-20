package io.github.dataconnector.connector.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.github.dataconnector.spi.DataSink;
import io.github.dataconnector.spi.DataSource;
import io.github.dataconnector.spi.DataStreamSink;
import io.github.dataconnector.spi.DataStreamSource;
import io.github.dataconnector.spi.model.ConnectorContext;
import io.github.dataconnector.spi.model.ConnectorMetadata;
import io.github.dataconnector.spi.model.ConnectorResult;
import io.github.dataconnector.spi.stream.StreamCancellable;
import io.github.dataconnector.spi.stream.StreamObserver;
import io.github.dataconnector.spi.stream.StreamWriter;

public class CsvConnector implements DataSource, DataSink, DataStreamSource, DataStreamSink {

    private static final Logger logger = LoggerFactory.getLogger(CsvConnector.class);

    private final CsvMapper csvMapper = new CsvMapper();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return "csv";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorMetadata getMetadata() {
        return ConnectorMetadata.builder()
                .name("CSV Connector")
                .description("CSV Connector is a connector that reads and writes CSV files")
                .version("0.0.1")
                .author("Hai Pham Ngoc <ngochai285nd@gmail.com>")
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> validateConfiguration(ConnectorContext context) {
        List<String> errors = new ArrayList<>();
        Map<String, Object> configuration = context.getConfiguration();
        if (configuration == null
                || (!configuration.containsKey("file_path") && !configuration.containsKey("input_stream"))) {
            errors.add("Missing source: either 'file_path' or 'input_stream' is required");
        }
        return errors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorResult read(ConnectorContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        // Get the start row and limit configuration
        int startRow = context.getConfiguration("start_row", Integer.class).orElse(0);
        int limit = context.getConfiguration("limit", Integer.class).orElse(-1);

        // Parse the included columns configuration
        Set<Integer> includedColumns = parseIncludedColumns(context);

        // Create the reader for the CSV file and skip the initial rows
        BufferedReader reader = createReader(context);
        for (int i = 0; i < startRow; i++) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
        }

        try {
            CsvSchema schema = createSchema(context);
            configureMapper(context);

            MappingIterator<Map<String, Object>> iterator = csvMapper.readerFor(Map.class)
                    .with(schema)
                    .readValues(reader);

            List<Map<String, Object>> records = new ArrayList<>();
            while (iterator.hasNext()) {
                if (limit != -1 && records.size() >= limit) {
                    break;
                }
                Map<String, Object> rawRecord = iterator.next();
                records.add(filterRecord(rawRecord, includedColumns));
            }

            return ConnectorResult.builder()
                    .success(true)
                    .message("Successfully read " + records.size() + " records from CSV file")
                    .recordsProcessed(records.size())
                    .records(records)
                    .executionTimeMillis(System.currentTimeMillis() - startTime)
                    .build();
        } catch (Exception e) {
            logger.error("Error parsing CSV:", e);
            return ConnectorResult.builder()
                    .success(false)
                    .message("Parsing CSV failed: " + e.getMessage())
                    .build();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamCancellable startStream(ConnectorContext context, StreamObserver observer) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);

        Set<Integer> includedColumns = parseIncludedColumns(context);

        // Submit the stream reader task to the executor
        executor.submit(() -> {
            BufferedReader reader = null;
            try {
                logger.info("Starting CSV stream reader thread");

                // Create the reader for the CSV file and skip the initial rows
                reader = createReader(context);
                int startRow = context.getConfiguration("start_row", Integer.class).orElse(0);
                for (int i = 0; i < startRow; i++) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                }

                // Create the schema for the CSV file
                CsvSchema schema = createSchema(context);

                // Configure the mapper for the CSV file
                configureMapper(context);

                // Create the iterator for the CSV file
                MappingIterator<Map<String, Object>> iterator = csvMapper.readerFor(Map.class)
                        .with(schema)
                        .readValues(reader);
                while (iterator.hasNext() && running.get()) {
                    Map<String, Object> rawRecord = iterator.next();
                    observer.onNext(filterRecord(rawRecord, includedColumns));
                }

                logger.info("CSV stream reader thread completed");
                observer.onComplete();
            } catch (Exception e) {
                logger.error("Error in CSV stream reader thread:", e);
                observer.onError(e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                        logger.info("CSV reader closed");
                    } catch (IOException e) {
                        logger.warn("Error closing CSV reader:", e);
                    }
                }
            }
        });

        // Return a cancellable that stops the stream when requested
        return () -> {
            logger.info("Requesting stream cancellation");
            running.set(false);
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorResult write(ConnectorContext context, List<Map<String, Object>> data) throws Exception {
        long startTime = System.currentTimeMillis();

        if (data == null || data.isEmpty()) {
            return ConnectorResult.builder()
                    .success(false)
                    .message("No data to write")
                    .recordsProcessed(0)
                    .executionTimeMillis(System.currentTimeMillis() - startTime)
                    .build();
        }

        try (StreamWriter writer = createWriter(context)) {
            writer.writeBatch(data);
        } catch (Exception e) {
            logger.error("Error writing CSV file:", e);
            return ConnectorResult.builder()
                    .success(false)
                    .message("Failed to write CSV file: " + e.getMessage())
                    .build();
        }

        return ConnectorResult.builder()
                .success(true)
                .message("Successfully wrote " + data.size() + " records to CSV file")
                .recordsProcessed(data.size())
                .executionTimeMillis(System.currentTimeMillis() - startTime)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamWriter createWriter(ConnectorContext context) throws IOException {
        return new CsvStreamWriter(context);
    }

    /**
     * Creates a CSV schema based on the connector context configuration.
     * 
     * @param context The connector context containing configuration settings.
     * @return A {@link CsvSchema} object configured according to the context
     *         settings.
     */
    private CsvSchema createSchema(ConnectorContext context) {
        char delimiter = context.getConfiguration("delimiter", String.class).orElse(",").charAt(0);
        char quoteChar = context.getConfiguration("quote_char", String.class).orElse("\"").charAt(0);
        boolean withHeader = context.getConfiguration("use_first_row_as_header", Boolean.class).orElse(true);

        CsvSchema.Builder schemaBuilder = CsvSchema.builder()
                .setColumnSeparator(delimiter)
                .setQuoteChar(quoteChar);
        if (withHeader) {
            schemaBuilder.setUseHeader(true);
        }
        return schemaBuilder.build();
    }

    /**
     * Configures the CSV mapper based on the connector context configuration.
     * 
     * @param context The connector context containing configuration settings.
     */
    private void configureMapper(ConnectorContext context) {
        boolean trimSpaces = context.getConfiguration("trim_spaces", Boolean.class).orElse(false);
        if (trimSpaces) {
            csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        } else {
            csvMapper.disable(CsvParser.Feature.TRIM_SPACES);
        }

        boolean skipEmptyRows = context.getConfiguration("skip_empty_rows", Boolean.class).orElse(true);
        if (skipEmptyRows) {
            csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        } else {
            csvMapper.disable(CsvParser.Feature.SKIP_EMPTY_LINES);
        }

        csvMapper.enable(CsvParser.Feature.ALLOW_COMMENTS);
    }

    /**
     * Creates a BufferedReader for reading CSV data from a file or input data.
     * 
     * @param context The connector context containing configuration settings.
     * @return A {@link BufferedReader} object for reading CSV data.
     * @throws Exception If there is an error creating the reader.
     */
    private BufferedReader createReader(ConnectorContext context) throws Exception {
        String filePath = context.getConfiguration("file_path", String.class).orElse(null);
        Object inputStreamObject = context.getConfiguration().get("input_stream");
        String charsetName = context.getConfiguration("charset", String.class).orElse("UTF-8");

        Charset charset;
        try {
            charset = Charset.forName(charsetName);
        } catch (Exception e) {
            throw new Exception("Invalid charset: " + charsetName);
        }

        InputStream inputStream;
        if (inputStreamObject instanceof InputStream) {
            inputStream = (InputStream) inputStreamObject;
        } else if (filePath != null && !filePath.isBlank()) {
            File file = new File(filePath);
            if (!file.exists()) {
                URL url = getClass().getClassLoader().getResource(filePath);
                if (url != null) {
                    inputStream = url.openStream();
                    logger.info("Loading CSV file from URL: {}", url.toString());
                } else {
                    throw new IllegalArgumentException("File not found: " + filePath);
                }
            } else {
                inputStream = new FileInputStream(file);
            }
        } else {
            throw new IllegalArgumentException("No valid input data provided (file_path or input_data is required)");
        }

        return new BufferedReader(new InputStreamReader(inputStream, charset));

    }

    /**
     * Parses the included columns configuration from the connector context.
     * 
     * @param context The connector context containing configuration settings.
     * @return A set of column indices to include, or null if all columns should
     *         be read.
     */
    private Set<Integer> parseIncludedColumns(ConnectorContext context) {
        Object columnsConfig = context.getConfiguration().get("columns");
        if (columnsConfig == null) {
            return null;
        }

        Set<Integer> indices = new HashSet<>();
        try {
            if (columnsConfig instanceof String) {
                String[] parts = ((String) columnsConfig).split(",");
                for (String part : parts) {
                    part = part.trim();
                    if (part.contains("-")) {
                        String[] range = part.split("-");
                        if (range.length != 2) {
                            throw new IllegalArgumentException("Invalid column range: " + part);
                        }
                        int start = Integer.parseInt(range[0].trim());
                        int end = Integer.parseInt(range[1].trim());
                        for (int i = start; i <= end; i++) {
                            indices.add(i);
                        }
                    } else {
                        int index = Integer.parseInt(part.trim());
                        indices.add(index);
                    }
                }
            } else if (columnsConfig instanceof List) {
                for (Object item : (List<?>) columnsConfig) {
                    indices.add(Integer.parseInt(item.toString()));
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse 'columns' configuration. Reading all columns.", e);
            return null;
        }
        return indices.isEmpty() ? null : indices;
    }

    /**
     * Filters a record to only include the specified columns.
     * 
     * @param rawRecord       The raw record to filter.
     * @param includedColumns The columns to include.
     * @return The filtered record.
     */
    private Map<String, Object> filterRecord(Map<String, Object> rawRecord, Set<Integer> includedColumns) {
        if (includedColumns == null || rawRecord == null || rawRecord.isEmpty()) {
            return rawRecord;
        }

        Map<String, Object> filteredRecord = new LinkedHashMap<>();
        int currentIndex = 0;
        for (Map.Entry<String, Object> entry : rawRecord.entrySet()) {
            if (includedColumns.contains(currentIndex)) {
                filteredRecord.put(entry.getKey(), entry.getValue());
            }
            currentIndex++;
        }
        return filteredRecord;
    }

    private class CsvStreamWriter implements StreamWriter {
        private final ConnectorContext context;
        private SequenceWriter sequenceWriter;
        private Writer outputWriter;
        private boolean isClosed = false;

        public CsvStreamWriter(ConnectorContext context) {
            this.context = context;
        }

        @Override
        public void close() throws IOException {
            if (isClosed) {
                return;
            }
            if (sequenceWriter != null) {
                sequenceWriter.close();
            } else if (outputWriter != null) {
                outputWriter.close();
            }
        }

        @Override
        public void writeBatch(List<Map<String, Object>> records) throws IOException {
            if (isClosed) {
                throw new IOException("Stream writer is closed");
            }
            if (records == null || records.isEmpty()) {
                return;
            }

            if (sequenceWriter == null) {
                initializeWriter(records.get(0).keySet());
            }

            for (Map<String, Object> record : records) {
                sequenceWriter.write(record);
            }

            sequenceWriter.flush();
        }

        private void initializeWriter(Set<String> headers) throws IOException {
            String filePath = context.getConfiguration("file_path", String.class).orElse(null);
            Object outputStreamObject = context.getConfiguration().get("output_stream");
            char delimiter = context.getConfiguration("delimiter", String.class).orElse(",").charAt(0);
            char quoteChar = context.getConfiguration("quote_char", String.class).orElse("\"").charAt(0);
            boolean withHeader = context.getConfiguration("use_first_row_as_header", Boolean.class).orElse(true);
            boolean append = context.getConfiguration("append", Boolean.class).orElse(false);
            String charsetName = context.getConfiguration("charset", String.class).orElse("UTF-8");

            Charset charset;
            try {
                charset = Charset.forName(charsetName);
            } catch (Exception e) {
                throw new IOException("Invalid charset: " + charsetName);
            }

            CsvSchema.Builder schemaBuilder = CsvSchema.builder()
                    .setColumnSeparator(delimiter)
                    .setQuoteChar(quoteChar);

            for (String header : headers) {
                schemaBuilder.addColumn(header);
            }

            boolean shouldWriteHeader = withHeader;
            if (filePath != null && append) {
                File file = new File(filePath);
                if (file.exists() && file.length() > 0) {
                    shouldWriteHeader = false;
                }
            }

            if (shouldWriteHeader) {
                schemaBuilder.setUseHeader(true);
            } else {
                //
            }

            CsvSchema schema = schemaBuilder.build();

            if (outputStreamObject instanceof OutputStream) {
                this.outputWriter = new OutputStreamWriter((OutputStream) outputStreamObject, charset);
            } else if (filePath != null) {
                File file = new File(filePath);
                if (file.getParentFile() != null && !file.getParentFile().exists()) {
                    if (!file.getParentFile().mkdirs()) {
                        throw new IOException(
                                "Failed to create parent directory: " + file.getParentFile().getAbsolutePath());
                    }
                }
                this.outputWriter = new OutputStreamWriter(new FileOutputStream(file, append), charset);
            } else {
                throw new IOException("Target not found: either output_stream or file_path must be provided");
            }

            this.sequenceWriter = csvMapper.writer(schema).writeValues(outputWriter);
        }
    }

}

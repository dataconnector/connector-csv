package io.github.dataconnector.connector.csv;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.github.dataconnector.spi.DataSink;
import io.github.dataconnector.spi.DataSource;
import io.github.dataconnector.spi.model.ConnectorContext;
import io.github.dataconnector.spi.model.ConnectorMetadata;
import io.github.dataconnector.spi.model.ConnectorResult;

public class CsvConnector implements DataSource, DataSink {

    private static final Logger logger = LoggerFactory.getLogger(CsvConnector.class);

    private final CsvMapper csvMapper = new CsvMapper();

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
                || (!configuration.containsKey("file_path") && !configuration.containsKey("input_data"))) {
            errors.add("Missing source: either 'file_path' or 'input_data' is required");
        }
        return errors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorResult read(ConnectorContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        String filePath = context.getConfiguration("file_path", String.class).orElse(null);
        Object inputData = context.getConfiguration().get("input_data");

        char delimiter = context.getConfiguration("delimiter", String.class).orElse(",").charAt(0);
        char quoteChar = context.getConfiguration("quote_char", String.class).orElse("\"").charAt(0);
        boolean withHeader = context.getConfiguration("use_first_row_as_header", Boolean.class).orElse(true);
        boolean skipEmptyRows = context.getConfiguration("skip_empty_rows", Boolean.class).orElse(true);
        boolean trimSpaces = context.getConfiguration("trim_spaces", Boolean.class).orElse(false);
        int startRow = context.getConfiguration("start_row", Integer.class).orElse(0);
        String charsetName = context.getConfiguration("charset", String.class).orElse("UTF-8");

        Charset charset;
        try {
            charset = Charset.forName(charsetName);
        } catch (Exception e) {
            return ConnectorResult.builder().success(false).message("Invalid charset: " + charsetName).build();
        }

        CsvSchema.Builder schemaBuilder = CsvSchema.builder()
                .setColumnSeparator(delimiter)
                .setQuoteChar(quoteChar);
        if (withHeader) {
            schemaBuilder.setUseHeader(true);
        }

        CsvSchema schema = schemaBuilder.build();
        if (trimSpaces) {
            csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        } else {
            csvMapper.disable(CsvParser.Feature.TRIM_SPACES);
        }

        if (skipEmptyRows) {
            csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        } else {
            csvMapper.disable(CsvParser.Feature.SKIP_EMPTY_LINES);
        }

        csvMapper.enable(CsvParser.Feature.ALLOW_COMMENTS);

        InputStream inputStream;

        if (inputData instanceof byte[]) {
            inputStream = new ByteArrayInputStream((byte[]) inputData);
        } else if (inputData instanceof String) {
            inputStream = new ByteArrayInputStream(((String) inputData).getBytes(charset));
        } else if (filePath != null && !filePath.isBlank()) {
            File file = new File(filePath);
            if (!file.exists()) {
                URL url = getClass().getClassLoader().getResource(filePath);
                if (url != null) {
                    inputStream = url.openStream();
                    logger.info("Loading CSV file from URL: {}", url.toString());
                } else {
                    return ConnectorResult.builder()
                            .success(false)
                            .message("File not found: " + filePath)
                            .build();
                }
            } else {
                inputStream = new FileInputStream(file);
                logger.info("Loading CSV file from file: {}", file.getAbsolutePath());
            }
        } else {
            return ConnectorResult.builder()
                    .success(false)
                    .message("No valid input data provided (file_path or input_data is required)")
                    .build();
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset));
        for (int i = 0; i < startRow; i++) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
        }

        try {
            MappingIterator<Map<String, Object>> iterator = csvMapper.readerFor(Map.class)
                    .with(schema)
                    .readValues(reader);
            List<Map<String, Object>> records = iterator.readAll();
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

    @Override
    public ConnectorResult write(ConnectorContext context, List<Map<String, Object>> data) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }
}

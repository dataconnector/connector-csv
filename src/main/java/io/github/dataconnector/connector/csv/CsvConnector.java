package io.github.dataconnector.connector.csv;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.github.dataconnector.spi.DataSink;
import io.github.dataconnector.spi.DataSource;
import io.github.dataconnector.spi.model.ConnectorContext;
import io.github.dataconnector.spi.model.ConnectorMetadata;
import io.github.dataconnector.spi.model.ConnectorResult;

public class CsvConnector implements DataSource, DataSink {

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
        boolean withHeader = context.getConfiguration("with_header", Boolean.class).orElse(true);

        CsvSchema schema = CsvSchema.emptySchema()
                .withColumnSeparator(delimiter);
        if (withHeader) {
            schema = schema.withHeader();
        }

        MappingIterator<Map<String, Object>> iterator;
        if (inputData instanceof byte[]) {
            iterator = csvMapper.readerFor(Map.class).with(schema)
                    .readValues(new ByteArrayInputStream((byte[]) inputData));
        } else if (inputData instanceof String) {
            iterator = csvMapper.readerFor(Map.class).with(schema)
                    .readValues(((String) inputData).getBytes(StandardCharsets.UTF_8));
        } else if (filePath != null) {
            iterator = csvMapper.readerFor(Map.class).with(schema)
                    .readValues(new File(filePath));
        } else {
            return ConnectorResult.builder()
                    .success(false)
                    .message("No valid input data provided (file_path or input_data is required)")
                    .build();
        }

        List<Map<String, Object>> records = iterator.readAll();
        return ConnectorResult.builder()
                .success(true)
                .message("Successfully read " + records.size() + " records from CSV file")
                .recordsProcessed(records.size())
                .records(records)
                .executionTimeMillis(System.currentTimeMillis() - startTime)
                .build();
    }

    @Override
    public ConnectorResult write(ConnectorContext context, List<Map<String, Object>> data) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }
}

package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.data.Schema;


public class JsonFileArrowTest {
  public static void main(String[] args)
      throws Exception {
    String filePath = "/Users/kharekartik/Documents/Workspace/Arrow/rawData/100k-864.json";

    Schema pinotSchema = Schema.fromString("{\n" + "  \"schemaName\": \"exampleSchema\",\n" + "  \"enableColumnBasedNullHandling\": false,\n"
        + "  \"dimensionFieldSpecs\": [\n" + "    {\n" + "      \"name\": \"low_cardinality_string\",\n"
        + "      \"dataType\": \"STRING\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"high_cardinality_string\",\n" + "      \"dataType\": \"STRING\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"low_cardinality_int\",\n"
        + "      \"dataType\": \"INT\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"high_cardinality_int\",\n" + "      \"dataType\": \"INT\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"low_cardinality_long\",\n"
        + "      \"dataType\": \"LONG\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"high_cardinality_long\",\n" + "      \"dataType\": \"LONG\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"low_cardinality_float\",\n"
        + "      \"dataType\": \"FLOAT\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"high_cardinality_float\",\n" + "      \"dataType\": \"FLOAT\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"low_cardinality_double\",\n"
        + "      \"dataType\": \"DOUBLE\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"high_cardinality_double\",\n" + "      \"dataType\": \"DOUBLE\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"boolean_field\",\n"
        + "      \"dataType\": \"BOOLEAN\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"timestamp_field\",\n" + "      \"dataType\": \"TIMESTAMP\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"json_field\",\n"
        + "      \"dataType\": \"JSON\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"bytes_field\",\n" + "      \"dataType\": \"BYTES\",\n" + "      \"notNull\": false\n"
        + "    }\n" + "  ],\n" + "  \"metricFieldSpecs\": [\n" + "    {\n" + "      \"name\": \"metric_int\",\n"
        + "      \"dataType\": \"INT\",\n" + "      \"notNull\": false\n" + "    },\n" + "    {\n"
        + "      \"name\": \"metric_long\",\n" + "      \"dataType\": \"LONG\",\n" + "      \"notNull\": false\n"
        + "    },\n" + "    {\n" + "      \"name\": \"metric_float\",\n" + "      \"dataType\": \"FLOAT\",\n"
        + "      \"notNull\": false\n" + "    },\n" + "    {\n" + "      \"name\": \"metric_double\",\n"
        + "      \"dataType\": \"DOUBLE\",\n" + "      \"notNull\": false\n" + "    }\n" + "  ],\n"
        + "  \"dateTimeFieldSpecs\": [\n" + "    {\n" + "      \"name\": \"event_time\",\n"
        + "      \"dataType\": \"TIMESTAMP\",\n" + "      \"notNull\": false,\n"
        + "      \"format\": \"1:MILLISECONDS:EPOCH\",\n" + "      \"granularity\": \"1:MILLISECONDS\"\n" + "    }\n"
        + "  ]\n" + "}\n" + "12:39\n");

    List<String> sortedColumns = new ArrayList<>();
    sortedColumns.add("low_cardinality_long");

    // create a new file
//    File offsetFile = new File("/Users/aishik/Work/rawData/offsetFile");

    try {
      File dataFile = new File("/Users/aishik/Work/rawData/dataFile.arrow");
      JsonNodeArrowWriter jsonNodeArrowWriter = new JsonNodeArrowWriter(null, dataFile, pinotSchema, sortedColumns);

      // Create an instance of ObjectMapper
      ObjectMapper objectMapper = new ObjectMapper();

      // Create a Scanner to read the file line by line
      Scanner scanner = new Scanner(new File(filePath));

      // Read the JSONL file line by line
      while (scanner.hasNextLine()) {
        String jsonLine = scanner.nextLine();

        // Parse the JSON line into a JsonNode object
        JsonNode jsonNode = objectMapper.readTree(jsonLine);

        // Process the JSON data
        jsonNodeArrowWriter.write(jsonNode);
      }

      // Close the scanner
      scanner.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

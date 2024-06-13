package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import one.profiler.AsyncProfiler;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public class JsonlToArrowInt {
  public static void main(String[] args) {

    AsyncProfiler profiler = AsyncProfiler.getInstance();

    String jsonlFilePath = "/Users/kharekartik/Documents/Workspace/Arrow/100k-864.json";
    String arrowFilePath = "/Users/kharekartik/Documents/Workspace/Arrow/output.arrow";

    try (BufferAllocator allocator = new RootAllocator(2 * 1024 * 1024L)) {
      // Create schema
      List<Field> fields = new ArrayList<>();
      String columnName = "low_cardinality_int";
      fields.add(new Field(columnName, FieldType.nullable(new ArrowType.Int(32, true)), null)); // Example field
      // Add more fields according to your JSON structure

      Schema schema = new Schema(fields);

      // Create VectorSchemaRoot
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

      // Read JSONL file
      ObjectMapper mapper = new ObjectMapper();
      int batchRowCount = 0;
      IntVector lowCardinalityString = (IntVector) root.getVector(columnName);
      try (BufferedReader reader = new BufferedReader(new FileReader(jsonlFilePath))) {
        String line;
        while ((line = reader.readLine()) != null) {
          JsonNode jsonNode = mapper.readTree(line);
          // Populate the VectorSchemaRoot
          int colValue = jsonNode.get(columnName).asInt();
          (lowCardinalityString).setSafe(batchRowCount, colValue);

          // Populate other fields

          root.setRowCount(root.getRowCount() + 1);
          batchRowCount++;
//          System.out.printf("Processed %d rows: Current value: %s\n", batchRowCount, new String(lowCardinalityString.get(batchRowCount - 1), StandardCharsets.UTF_8));
        }
      }

      // time taken to sort array
//      List<String> arrayList = new ArrayList<>();
//      for (int i=0; i < batchRowCount; i++) {
//        arrayList.add(new String((lowCardinalityString).get(i), StandardCharsets.UTF_8));
//      }

      long startTime = System.currentTimeMillis();
//      Collections.sort(arrayList);
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));


      startTime = System.currentTimeMillis();
      try (IntVector indexVector = new IntVector("intVector", allocator)) {
        indexVector.allocateNew(batchRowCount);
        for (int i = 0; i < batchRowCount; i++) {
          indexVector.setSafe(i, i);
        }
        indexVector.setValueCount(batchRowCount);
        System.out.println("Time taken to create index vector: " + (System.currentTimeMillis() - startTime));
        IndexSorter<IntVector> sorter = new IndexSorter<>();
        VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(
            lowCardinalityString);

        String profilerFileName = "ArrowSorterWallInt-" + System.currentTimeMillis();
        profiler.execute(String.format("start,event=wall,file=%s.html", profilerFileName));
        sorter.sort(lowCardinalityString, indexVector, comparator);
        System.out.println("Time taken to sort index vector: " + (System.currentTimeMillis() - startTime));
        profiler.execute(String.format("stop,file=%s.html", profilerFileName));

        try (IntVector sortedVector = new IntVector("sorted", allocator)) {
          sortedVector.allocateNew(batchRowCount);
          for (int i = 0; i < batchRowCount; i++) {
            sortedVector.setSafe(i, (lowCardinalityString).get(indexVector.get(i)));
//            System.out.printf("Processed %d rows: Current value: %s\n", i, (lowCardinalityString).get(indexVector.get(i)));
          }
          sortedVector.setValueCount(batchRowCount);

          // Replace the original vector with the sorted vector
          root.clear();
          root.setRowCount(batchRowCount);
          root.addVector(0, sortedVector);
        }

      }

      // Write to Arrow file
      try (FileOutputStream fileOutputStream = new FileOutputStream(arrowFilePath);
          ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
        System.out.printf("Arrow file written to %s\n", arrowFilePath);
      }
      root.close();

    } catch (IOException e) {
      e.printStackTrace();
    } finally {

    }
  }
}










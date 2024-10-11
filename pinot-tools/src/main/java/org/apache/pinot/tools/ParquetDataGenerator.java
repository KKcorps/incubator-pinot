package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.*;
import java.io.FileInputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import one.profiler.AsyncProfiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.*;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.*;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import org.apache.pinot.plugin.filesystem.S3Config;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;


public class ParquetDataGenerator {

  public static AtomicLong _numRecordsGenerated = new AtomicLong(0);
  // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html, the part number must be an integer
  // between 1 and 10000, inclusive; and the min part size allowed is 5MiB, except the last one.
  private static final long MULTI_PART_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final int MULTI_PART_UPLOAD_MAX_PART_NUM = 10000;

  public static void main(String[] args) throws Exception {
    int numRecords = 0;
    String schemaPath = "golden_schema.json";
    int numProcesses = Runtime.getRuntime().availableProcessors();
    String s3Bucket = "";
    String s3Folder = "";

    if (args.length < 1) {
      System.err.println("Usage: java DataGenerator <num_records> [--schema <schema_path>] [--num_processes <num_processes>] --s3_bucket <bucket> [--s3_folder <folder>]");
      System.exit(1);
    }

    numRecords = Integer.parseInt(args[0]);

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--schema")) {
        schemaPath = args[++i];
      } else if (args[i].equals("--num_processes")) {
        numProcesses = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--s3_bucket")) {
        s3Bucket = args[++i];
      } else if (args[i].equals("--s3_folder")) {
        s3Folder = args[++i];
      }
    }

    if (s3Bucket.isEmpty()) {
      System.err.println("Error: --s3_bucket is required");
      System.exit(1);
    }

    // Load schema
    Schema pinotSchema = loadSchema(schemaPath);
    // Create Parquet schema
    MessageType parquetSchema = createParquetSchema(pinotSchema);
    System.out.println("Parquet Schema:");
    System.out.println(parquetSchema);

    // Generate data and upload to S3
    generateDataAndUpload(numRecords, numProcesses, pinotSchema, parquetSchema, s3Bucket, s3Folder);
  }

  public static Schema loadSchema(String schemaPath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(schemaPath), Schema.class);
  }

  public static MessageType createParquetSchema(Schema pinotSchema) {
    List<Type> fields = new ArrayList<>();

    Collection<FieldSpec> allFieldSpecs = pinotSchema.getAllFieldSpecs();

    for (FieldSpec fieldSpec : allFieldSpecs) {
      PrimitiveType.PrimitiveTypeName parquetType;
      OriginalType originalType = null;

      switch (fieldSpec.getDataType().toString()) {
        case "INT":
          parquetType = PrimitiveType.PrimitiveTypeName.INT32;
          break;
        case "LONG":
          parquetType = PrimitiveType.PrimitiveTypeName.INT64;
          break;
        case "FLOAT":
          parquetType = PrimitiveType.PrimitiveTypeName.FLOAT;
          break;
        case "DOUBLE":
          parquetType = PrimitiveType.PrimitiveTypeName.DOUBLE;
          break;
        case "BOOLEAN":
          parquetType = PrimitiveType.PrimitiveTypeName.BOOLEAN;
          break;
        case "STRING":
          parquetType = PrimitiveType.PrimitiveTypeName.BINARY;
          originalType = OriginalType.UTF8;
          break;
        case "BYTES":
          parquetType = PrimitiveType.PrimitiveTypeName.BINARY;
          break;
        case "JSON":
          parquetType = PrimitiveType.PrimitiveTypeName.BINARY;
          originalType = OriginalType.UTF8;
          break;
        case "BIG_DECIMAL":
          parquetType = PrimitiveType.PrimitiveTypeName.BINARY;
          originalType = OriginalType.UTF8;
          break;
        case "TIMESTAMP":
          parquetType = PrimitiveType.PrimitiveTypeName.INT64;
          originalType = OriginalType.TIMESTAMP_MILLIS;
          break;
        default:
          parquetType = PrimitiveType.PrimitiveTypeName.BINARY;
          originalType = OriginalType.UTF8;
          break;
      }

      Type.Repetition repetition = fieldSpec.isSingleValueField() ? Type.Repetition.OPTIONAL : Type.Repetition.REPEATED;

      PrimitiveType primitiveType = new PrimitiveType(repetition, parquetType, fieldSpec.getName(), originalType);

      fields.add(primitiveType);
    }

    MessageType schema = new MessageType("schema", fields);
    return schema;
  }

  public static void generateDataAndUpload(int numRecords, int numProcesses, Schema pinotSchema, MessageType parquetSchema, String s3Bucket, String s3Folder) throws Exception {
    int batchSize = 100000;
    int numBatches = (numRecords + batchSize - 1) / batchSize;

    ExecutorService executor = Executors.newFixedThreadPool(1);

    int batchesPerProcess = numBatches / numProcesses;
    int extraBatches = numBatches % numProcesses;

    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numProcesses; i++) {
      int batches = batchesPerProcess + (i < extraBatches ? 1 : 0);
      int processId = i + 1;
      Runnable worker = new DataGeneratorWorker(processId, pinotSchema, parquetSchema, batchSize, batches, s3Bucket, s3Folder);
      futures.add(executor.submit(worker));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    executor.shutdownNow();
    System.out.println("Generated and uploaded " + numRecords + " records using " + numProcesses + " processes");
  }
}

class DataGeneratorWorker implements Runnable {
  private int processId;
  private Schema pinotSchema;
  private MessageType parquetSchema;
  private int batchSize;
  private int numBatches;
  private String s3Bucket;
  private String s3Folder;

  // Create S3Client
  private S3Client s3Client = S3Client.builder().region(Region.US_EAST_1) // Replace with your desired region
      .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create("XXXX", "XXXX"))).build();

  // From https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html, the part number must be an integer
  // between 1 and 10000, inclusive; and the min part size allowed is 5MiB, except the last one.
  private static final long MULTI_PART_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final int MULTI_PART_UPLOAD_MAX_PART_NUM = 10000;

  public DataGeneratorWorker(int processId, Schema pinotSchema, MessageType parquetSchema, int batchSize, int numBatches, String s3Bucket, String s3Folder) {
    this.processId = processId;
    this.pinotSchema = pinotSchema;
    this.parquetSchema = parquetSchema;
    this.batchSize = batchSize;
    this.numBatches = numBatches;
    this.s3Bucket = s3Bucket;
    this.s3Folder = s3Folder;
  }

  @Override
  public void run() {
    try {
      boolean generateProfilingOutput = true;
      AsyncProfiler profiler = AsyncProfiler.getInstance();
      for (int batchNum = 0; batchNum < numBatches; batchNum++) {
        String profilerFileName = "profiler-output-parquet-golden-dataset-" + processId + "-" + batchNum + ".txt";
        if (generateProfilingOutput) {
          profiler.execute("start,event=wall,file=" + profilerFileName);
        }

        // Generate data
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(parquetSchema);
        long startTime = System.currentTimeMillis();
        List<Group> records = generateDataBatch(pinotSchema, batchSize, groupFactory);
        System.out.printf("Process %d: Generated %d records in %d ms\n", processId, batchSize, System.currentTimeMillis() - startTime);

        // Write to Parquet file
        startTime = System.currentTimeMillis();
        String fileName = "data_" + UUID.randomUUID().toString() + ".parquet";
        java.nio.file.Path tempFilePath = Paths.get(System.getProperty("java.io.tmpdir"), fileName);
        writeParquetFile(parquetSchema, records, tempFilePath);
        System.out.printf("Process %d: Wrote %d records to Parquet file in %d ms\n", processId, batchSize, System.currentTimeMillis() - startTime);

        // Upload to S3
        startTime = System.currentTimeMillis();
        uploadToS3(tempFilePath.toFile(), s3Bucket, s3Folder, fileName, records.size());
        System.out.printf("Process %d: Uploaded %d records to S3 in %d ms\n", processId, batchSize, System.currentTimeMillis() - startTime);

        // Delete temporary file
        tempFilePath.toFile().delete();

        if ((batchNum + 1) % 10 == 0) {
          System.out.println("Process " + processId + ": Generated " + (batchNum + 1) + "/" + numBatches + " batches");
        }

        if (generateProfilingOutput) {
          profiler.execute("stop,file=" + profilerFileName);
        }

        generateProfilingOutput = false;
      }

    } catch (Exception e) {
      System.err.println("Error in process " + processId + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  public List<Group> generateDataBatch(Schema pinotSchema, int batchSize, SimpleGroupFactory groupFactory) {
    List<Group> records = new ArrayList<>(batchSize);
    Random random = new Random();

    for (int i = 0; i < batchSize; i++) {
      Group group = groupFactory.newGroup();

      Collection<FieldSpec> allFieldSpecs = pinotSchema.getAllFieldSpecs();

      for (FieldSpec fieldSpec : allFieldSpecs) {
        String name = fieldSpec.getName();
        boolean isNullable = !fieldSpec.isNullable();
        boolean isSingleValue = fieldSpec.isSingleValueField();

        // Generate value
        Object value = generateRandomValue(fieldSpec, random);

        // Apply nullability
        if (isNullable && random.nextDouble() < 0.1) { // 10% chance of null
          value = null;
        }

        // Set value to group
        if (value != null) {
          if (isSingleValue) {
            setGroupValue(group, fieldSpec, value);
          } else {
            // For list fields, generate a list of values
            List<Object> values = generateRandomList(fieldSpec, random);
            for (Object v : values) {
              setGroupValue(group, fieldSpec, v);
            }
          }
        }
      }

      records.add(group);
    }

    return records;
  }

  public Object generateRandomValue(FieldSpec fieldSpec, Random random) {
    String dataType = fieldSpec.getDataType().toString();
    switch (dataType) {
      case "INT":
        return random.nextInt(30001);
      case "LONG":
        return (long) random.nextInt(2000000001);
      case "FLOAT":
        return random.nextFloat() * 30000.0f;
      case "DOUBLE":
        return random.nextDouble() * 2000000000.0;
      case "BOOLEAN":
        return random.nextBoolean();
      case "STRING":
        int maxLength = getStringLength(fieldSpec.getName(), fieldSpec);
        return generateRandomString(random, maxLength);
      case "BYTES":
        int length = random.nextInt(100) + 1;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
      case "JSON":
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key", random.nextInt(100) + 1);
        jsonMap.put("value", UUID.randomUUID().toString());
        try {
          ObjectMapper mapper = new ObjectMapper();
          return mapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
          return "{}";
        }
      case "BIG_DECIMAL":
        return new BigDecimal(random.nextInt(2000001) - 1000000).divide(new BigDecimal(100)).toString();
      case "TIMESTAMP":
        return System.currentTimeMillis();
      default:
        return null;
    }
  }

  public int getStringLength(String fieldName, FieldSpec fieldSpec) {
    if (fieldSpec.getMaxLength() > 0) {
      return fieldSpec.getMaxLength();
    } else if (fieldName.contains("Small")) {
      return 32;
    } else if (fieldName.contains("Medium")) {
      return 256;
    } else if (fieldName.contains("Large")) {
      return 1024;
    } else {
      return 100;
    }
  }

  public String generateRandomString(Random random, int maxLength) {
    int length = random.nextInt(maxLength) + 1;
    String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  public Object generateTimestamp(String formatSpec) {
    Date now = new Date();
    switch (formatSpec) {
      case "1:MILLISECONDS:EPOCH":
        return now.getTime();
      case "1:SECONDS:EPOCH":
        return now.getTime() / 1000;
      case "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss.SSSXXX":
        // Return formatted date string
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(now);
      case "1:MILLISECONDS:TIMESTAMP":
        return now.getTime();
      default:
        throw new IllegalArgumentException("Unsupported timestamp format: " + formatSpec);
    }
  }

  public List<Object> generateRandomList(FieldSpec fieldSpec, Random random) {
    int listLength = random.nextInt(5) + 1; // Lists of length 1 to 5
    List<Object> list = new ArrayList<>(listLength);
    for (int i = 0; i < listLength; i++) {
      list.add(generateRandomValue(fieldSpec, random));
    }
    return list;
  }

  public void setGroupValue(Group group, FieldSpec fieldSpec, Object value) {
    String name = fieldSpec.getName();
    if (value == null) return;

    switch (fieldSpec.getDataType().toString()) {
      case "INT":
        group.append(name, (int) value);
        break;
      case "LONG":
        group.append(name, (long) value);
        break;
      case "FLOAT":
        group.append(name, (float) value);
        break;
      case "DOUBLE":
        group.append(name, (double) value);
        break;
      case "BOOLEAN":
        group.append(name, (boolean) value);
        break;
      case "STRING":
      case "JSON":
      case "BIG_DECIMAL":
        group.append(name, value.toString());
        break;
      case "BYTES":
        group.append(name, Binary.fromConstantByteArray((byte[]) value));
        break;
      case "TIMESTAMP":
        if (value instanceof Long) {
          group.append(name, (long) value);
        } else if (value instanceof String) {
          group.append(name, value.toString());
        }
        break;
      default:
        break;
    }
  }

  public void writeParquetFile(MessageType schema, List<Group> records, java.nio.file.Path filePath) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    ParquetWriter<Group> writer = new ParquetWriter<>(
        new Path(filePath.toUri()),
        new GroupWriteSupport(),
        CompressionCodecName.SNAPPY,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetProperties.WriterVersion.PARQUET_1_0,
        conf);

    for (Group group : records) {
      writer.write(group);
    }
    writer.close();
  }


  public void uploadToS3(File file, String s3Bucket, String s3Folder, String fileName, int numRecords) {
    try {
      String s3Key = s3Folder.isEmpty() ? fileName : s3Folder + "/" + fileName;
      uploadFileInParts(file, s3Bucket, s3Key);

//      // Create PutObjectRequest
//      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
//          .bucket(s3Bucket)
//          .key(s3Key)
//          .build();
//
//      // Upload file
//      s3Client.putObject(putObjectRequest, java.nio.file.Path.of(file.getAbsolutePath()));
//
//      ParquetDataGenerator._numRecordsGenerated.addAndGet(numRecords);
//
//      System.out.println("Uploaded " + numRecords + " in " + s3Key + " to " + s3Bucket + " total: "
//          + ParquetDataGenerator._numRecordsGenerated.get());
    } catch (Exception e) {
      System.err.println("Failed to upload to S3: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void uploadFileInParts(File srcFile, String bucket, String prefix) throws Exception {

    CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
        .bucket(bucket)
        .key(prefix);

    CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(createMultipartUploadRequestBuilder.build());
    String uploadId = multipartUpload.uploadId();

    try (FileInputStream inputStream = new FileInputStream(srcFile)) {
      long totalUploaded = 0;
      long fileSize = srcFile.length();
      int partNum = 1;
      long partSizeToUse = 10 * 1024 * 1024; // 10MB
      if (partSizeToUse * S3Config.MULTI_PART_UPLOAD_MAX_PART_NUM < fileSize) {
        partSizeToUse =
            (fileSize + S3Config.MULTI_PART_UPLOAD_MAX_PART_NUM - 1) / S3Config.MULTI_PART_UPLOAD_MAX_PART_NUM;
      }

      List<CompletedPart> parts = new ArrayList<>();

      while (totalUploaded < srcFile.length()) {
        long nextPartSize = Math.min(partSizeToUse, fileSize - totalUploaded);
        UploadPartResponse uploadPartResponse = s3Client.uploadPart(
            UploadPartRequest.builder()
                .bucket(bucket)
                .key(prefix)
                .uploadId(uploadId)
                .partNumber(partNum)
                .build(),
            RequestBody.fromInputStream(inputStream, nextPartSize));

        parts.add(CompletedPart.builder().partNumber(partNum).eTag(uploadPartResponse.eTag()).build());
        totalUploaded += nextPartSize;

        System.out.println("Uploaded part " + partNum + " of size " + nextPartSize + ", with total uploaded " +
            totalUploaded + " and file size " + fileSize);

        partNum++;
      }

      s3Client.completeMultipartUpload(
          CompleteMultipartUploadRequest.builder()
              .uploadId(uploadId)
              .bucket(bucket)
              .key(prefix)
              .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
              .build());

    } catch (Exception e) {
      System.out.println("Failed to upload file " + srcFile + " to " + bucket + "/"  +  prefix + " in parts. Abort upload request: " + uploadId);
      s3Client.abortMultipartUpload(
          AbortMultipartUploadRequest.builder()
              .uploadId(uploadId)
              .bucket(bucket)
              .key(prefix)
              .build());
      throw e;
    }
  }

  // You'll need to implement these methods
  private String sanitizePath(String path) {
    // Implement path sanitization logic
    return path;
  }

  private URI getBase(URI uri) {
    // Implement logic to get base URI
    return uri;
  }
}


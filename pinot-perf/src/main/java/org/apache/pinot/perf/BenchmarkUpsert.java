package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.segment.local.upsert.RecordLocation;
import org.apache.pinot.segment.spi.IndexSegment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Segment objects also need to be created
 * Number of segments should be less than primary keys
 * Also need to insert valid docIds and primaryKeys in each segments
 * The number of docs should be greater than primary keys
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkUpsert {
  public static final int NUM_SEGMENTS = 1_000;
  private static final int KEY_COUNT = 1_000_000;
  private static final int DOC_COUNT = 5_000_000;
  private static final Random RANDOM = new Random();

  public class MockSegment {
    int[] validDocIds;
    String[] primaryKey;

    public int[] getValidDocIds() {
      return validDocIds;
    }

    public void setValidDocIds(int[] validDocIds) {
      this.validDocIds = validDocIds;
    }

    public String[] getPrimaryKey() {
      return primaryKey;
    }

    public void setPrimaryKey(String[] primaryKey) {
      this.primaryKey = primaryKey;
    }
  }

  public class MockRecordLocation {
    private final MockSegment _segment;
    private final int _docId;
    /** value used to denote the order */
    private final Comparable _comparisonValue;

    public MockRecordLocation(MockSegment indexSegment, int docId, Comparable comparisonValue) {
      _segment = indexSegment;
      _docId = docId;
      _comparisonValue = comparisonValue;
    }

    public MockSegment getSegment() {
      return _segment;
    }

    public int getDocId() {
      return _docId;
    }

    public Comparable getComparisonValue() {
      return _comparisonValue;
    }
  }

  final ConcurrentHashMap<Object, MockRecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();
  final List<MockSegment> _segmentList = new ArrayList<>();

  @Setup
  public void setUp() {
    int numDocsPerSegment = DOC_COUNT / NUM_SEGMENTS;
      for(int i = 0; i < NUM_SEGMENTS; i++) {
        MockSegment mockSegment = new MockSegment();
        int[] validDocIds = new int[numDocsPerSegment];
        String[] primaryKeys = new String[numDocsPerSegment];
        for(int j =0;j<numDocsPerSegment;j++){
            validDocIds[j] = RANDOM.nextInt(DOC_COUNT);
            primaryKeys[j] = generateRandomString(RANDOM.nextInt(50) + 50);
        }

        mockSegment.setPrimaryKey(primaryKeys);
        mockSegment.setValidDocIds(validDocIds);
        _segmentList.add(mockSegment);
      }

      for(MockSegment mockSegment: _segmentList) {
        int n = mockSegment.getPrimaryKey().length;
        for(int i=0;i<n;i++) {
          _primaryKeyToRecordLocationMap.put(mockSegment.getPrimaryKey()[i], new MockRecordLocation(mockSegment, mockSegment.getValidDocIds()[i], i));
        }
      }
  }

  @Benchmark
  public void benchmarkOldImpl(Blackhole blackhole) {

  }

  private String generateRandomString(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (RANDOM.nextInt(0x7F - 0x20) + 0x20);
    }
    return new String(bytes, UTF_8);
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkUpsert.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10)).warmupIterations(2).measurementTime(TimeValue.seconds(30))
        .measurementIterations(5).forks(1);

    new Runner(opt.build()).run();
  }
}

package org.apache.pinot.perf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.upsert.RecordLocation;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Segment objects also need to be created
 * Number of segments should be less than primary keys
 * Also need to insert valid docIds and primaryKeys in each segments
 * The number of docs should be greater than primary keys
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx4G", "-XX:MaxDirectMemorySize=8G"})
public class BenchmarkUpsert {
  public static final int NUM_SEGMENTS = 1_000;
  private static final int KEY_COUNT = 1_000_000;
  private static final int DOC_COUNT = 5_000_000;
  public static final int SEED = 42;

  public class MockSegment {
    MutableRoaringBitmap _validDocIds;
    String[] _primaryKey;
    Map<Integer, Integer> _docIdToIndex = new HashMap<>();

    public MutableRoaringBitmap getValidDocIds() {
      return _validDocIds;
    }

    public void setValidDocIds(int[] validDocIds) {
      ThreadSafeMutableRoaringBitmap mutableRoaringBitmap = new ThreadSafeMutableRoaringBitmap();
      for(int i=0;i<validDocIds.length;i++) {
        mutableRoaringBitmap.add(validDocIds[i]);
        _docIdToIndex.put(validDocIds[i], i);
      }
      _validDocIds = mutableRoaringBitmap.getMutableRoaringBitmap();
    }

    public String getPrimaryKey(int docId) {
      return _primaryKey[_docIdToIndex.get(docId)];
    }

    public String[] getAllPrimaryKeys() {
      return _primaryKey;
    }

    public void setPrimaryKey(String[] primaryKey) {
      this._primaryKey = primaryKey;
    }

    public Integer getIndexForDocId(int docId){
      return _docIdToIndex.get(docId);
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

  @Setup(Level.Iteration)
  public void setUp() {
    _segmentList.clear();
    _primaryKeyToRecordLocationMap.clear();
    Random random = new Random(SEED);

    int numDocsPerSegment = DOC_COUNT / NUM_SEGMENTS;
      for(int i = 0; i < NUM_SEGMENTS; i++) {
        MockSegment mockSegment = new MockSegment();
        int[] validDocIds = new int[numDocsPerSegment];
        String[] primaryKeys = new String[numDocsPerSegment];
        for(int j =0;j<numDocsPerSegment;j++){
            validDocIds[j] = random.nextInt(DOC_COUNT);
            primaryKeys[j] = generateRandomString(random.nextInt(50) + 50, random);
        }

        mockSegment.setPrimaryKey(primaryKeys);
        mockSegment.setValidDocIds(validDocIds);
        _segmentList.add(mockSegment);
      }

      for(MockSegment mockSegment: _segmentList) {
        PeekableIntIterator peekableIntIterator = mockSegment.getValidDocIds().getIntIterator();
        while(peekableIntIterator.hasNext()) {
          int docId = peekableIntIterator.next();
          _primaryKeyToRecordLocationMap.put(mockSegment.getPrimaryKey(docId), new MockRecordLocation(mockSegment, docId, System.currentTimeMillis()));
        }
      }
  }

  @Benchmark
  public void benchmarkOldImpl(Blackhole blackhole) {
      for(MockSegment segment: _segmentList) {
        blackhole.consume(removeSegmentOld(segment));
      }
  }

  @Benchmark
  public void benchmarkNewImpl(Blackhole blackhole) {
    for(MockSegment segment: _segmentList) {
      blackhole.consume(removeSegmentNew(segment));
    }
  }

  private String generateRandomString(int length, Random random) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (random.nextInt(0x7F - 0x20) + 0x20);
    }
    return new String(bytes, UTF_8);
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkUpsert.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5)).warmupIterations(1).measurementTime(TimeValue.seconds(10))
        .mode(Mode.AverageTime)
        .timeUnit(TimeUnit.MILLISECONDS)
        .measurementIterations(3).forks(1);

    new Runner(opt.build()).run();
  }


  public boolean removeSegmentOld(MockSegment segment) {
    _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
      if (recordLocation.getSegment() == segment) {
        // Check and remove to prevent removing the key that is just updated
        _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
      }
    });
    return true;
  }


  public boolean removeSegmentNew(MockSegment segment) {
    Iterator<Integer> iterator = segment.getValidDocIds().iterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      String primaryKey = segment.getPrimaryKey(docId);
      _primaryKeyToRecordLocationMap.computeIfPresent(primaryKey, (pk, recordLocation) -> {
        if (recordLocation.getSegment() == segment) {
          return null;
        }
        return recordLocation;
      });
    }
    return true;
  }
}

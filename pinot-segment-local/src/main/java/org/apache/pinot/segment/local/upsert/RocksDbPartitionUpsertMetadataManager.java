/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same comparison value (default to timestamp), the manager will preserve the latest
 * record based on the sequence number of the segment. If 2 records with the same comparison value are in the same
 * segment, the one with larger doc id will be preserved. Note that for tables with sorted column, the records will be
 * re-ordered when committing the segment, and we will use the re-ordered doc ids instead of the ingestion doc ids to
 * decide the record to preserve.
 *
 * <p>There will be short term inconsistency when updating the upsert metadata, but should be consistent after the
 * operation is done:
 * <ul>
 *   <li>
 *     When updating a new record, it first removes the doc id from the current location, then update the new location.
 *   </li>
 *   <li>
 *     When adding a new segment, it removes the doc ids from the current locations before the segment being added to
 *     the RealtimeTableDataManager.
 *   </li>
 *   <li>
 *     When replacing an existing segment, after the record location being replaced with the new segment, the following
 *     updates applied to the new segment's valid doc ids won't be reflected to the replaced segment's valid doc ids.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class RocksDbPartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  private static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final List<String> _primaryKeyColumns;
  private final FieldSpec _comparisonColumn;
  private final HashFunction _hashFunction;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final ServerMetrics _serverMetrics;
  private final Logger _logger;

  //TODO: find a way to persist this so that we can reuse the rocksDB state after server restarts
  private final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  private final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  private final AtomicInteger _segmentId = new AtomicInteger();
  private final WriteOptions _writeOptions;
  private final ReadOptions _readOptions;

  private final RocksDB _rocksDB;
  private Statistics _statistics;

  @VisibleForTesting
  final Set<IndexSegment> _replacedSegments = ConcurrentHashMap.newKeySet();

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  private long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  private int _numOutOfOrderEvents = 0;

  public RocksDbPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, List<String> primaryKeyColumns,
      FieldSpec comparisonColumn, HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler,
      ServerMetrics serverMetrics, File dataDir, UpsertConfig upsertConfig) throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());

    //TODO: Should we create only 1 rocksDB instance and use column families per table/partition
    String dirPath = Joiner.on("/").join("rocksdb", "partition-" + partitionId);
    File dir = new File(dataDir, dirPath);
    dir.mkdirs();

    _logger.info("Using storage path for rocksdb {}", dir);

    Options options = getOptimisedConfigForPointLookup(upsertConfig.getMetadataManagerConfigs());
    _rocksDB = RocksDB.open(options, dir.getAbsolutePath());
    _readOptions = new ReadOptions();
    _writeOptions = new WriteOptions();

    // Allows for faster puts while trading off recoverability in case of failure.
//    _writeOptions.setDisableWAL(true);
  }

  private Options getOptimisedConfigForPointLookup(Map<String, String> metadataManagerConfigs) {
    Options dbOptions = new Options();
    dbOptions.setCreateIfMissing(true);

    BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(5); // using latest file format
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
    blockBasedTableConfig.setDataBlockHashTableUtilRatio(0.75);
    blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
    blockBasedTableConfig.setBlockCache(new LRUCache(1024 * 1024 * 1024));

    blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
    dbOptions.useFixedLengthPrefixExtractor(10);

    // Pin and Cache blocks
    blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
    blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
    blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    blockBasedTableConfig.setOptimizeFiltersForMemory(true);

    dbOptions.setTableFormatConfig(blockBasedTableConfig);

    dbOptions.setMaxOpenFiles(-1); // allow multiple sstable files to be opened, good when you have to do random lreads
    dbOptions.setInfoLogLevel(InfoLogLevel.INFO_LEVEL); // disable logging
    dbOptions.setStatsDumpPeriodSec(300); // disable dumping rocksDB stats in logs
//    dbOptions.setUseFsync(false);
    dbOptions.setMemtableWholeKeyFiltering(true);
    dbOptions.setMemtablePrefixBloomSizeRatio(0.1);

    //Add row cache and use faster compression (default: snappy)
    dbOptions.setRowCache(new LRUCache(1024 * 1024 * 1024));
    dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);

    _statistics = new Statistics();
    dbOptions.setStatistics(_statistics);
    return dbOptions;
  }

  /**
   * Returns the primary key columns.
   */
  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  @Override
  public void addSegment(ImmutableSegment segment) {
    addSegment(segment, null, null);
  }

  @VisibleForTesting
  void addSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
//    _logger.info("Adding segment: {}, current primary key count: {}", segmentName,
//        _primaryKeyToRecordLocationMap.size());

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
          "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
          _tableNameWithType);
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      if (recordInfoIterator == null) {
        recordInfoIterator = getRecordInfoIterator(segment);
      }
      addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, recordInfoIterator, null, null);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
//    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
//    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
//        numPrimaryKeys);
//
//    _logger.info("Finished adding segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  private Iterator<RecordInfo> getRecordInfoIterator(ImmutableSegment segment) {
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    return new Iterator<RecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public RecordInfo next() {
        PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
        getPrimaryKey(segment, _docId, primaryKey);

        Object comparisonValue = segment.getValue(_docId, _comparisonColumn.getName());
        if (comparisonValue instanceof byte[]) {
          comparisonValue = new ByteArray((byte[]) comparisonValue);
        }
        return new RecordInfo(primaryKey, _docId++, (Comparable) comparisonValue);
      }
    };
  }

  private void getPrimaryKey(IndexSegment segment, int docId, PrimaryKey buffer) {
    Object[] values = buffer.getValues();
    int numPrimaryKeyColumns = values.length;
    for (int i = 0; i < numPrimaryKeyColumns; i++) {
      Object value = segment.getValue(docId, _primaryKeyColumns.get(i));
      if (value instanceof byte[]) {
        value = new ByteArray((byte[]) value);
      }
      values[i] = value;
    }
  }

  private void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      Iterator<RecordInfo> recordInfoIterator, @Nullable IndexSegment oldSegment,
      @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    String segmentName = segment.getSegmentName();
    int segmentId = getSegmentId(segment);

//    segment.enableUpsert(this, validDocIds);

    AtomicInteger numKeysInWrongSegment = new AtomicInteger();
    while (recordInfoIterator.hasNext()) {
      try {
        RecordInfo recordInfo = recordInfoIterator.next();
        byte[] primaryKey = HashUtils.hashPrimaryKeyAsBytes(recordInfo.getPrimaryKey(), _hashFunction);
        byte[] value = _rocksDB.get(_readOptions, primaryKey);
        if (value != null) {
          RecordLocationWithSegmentId currentRecordLocation = new RecordLocationWithSegmentId(value);

          // Existing primary key
          IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegment());
          int comparisonResult = recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

          // The current record is in the same segment
          // Update the record location when there is a tie to keep the newer record. Note that the record info
          // iterator will return records with incremental doc ids.
          if (currentSegment == segment) {
            if (comparisonResult >= 0) {
              validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
              _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
              continue;
            } else {
              continue;
            }
          }

          // The current record is in an old segment being replaced
          // This could happen when committing a consuming segment, or reloading a completed segment. In this
          // case, we want to update the record location when there is a tie because the record locations should
          // point to the new added segment instead of the old segment being replaced. Also, do not update the valid
          // doc ids for the old segment because it has not been replaced yet. We pass in an optional valid doc ids
          // snapshot for the old segment, which can be updated and used to track the docs not replaced yet.
          if (currentSegment == oldSegment) {
            if (comparisonResult >= 0) {
              validDocIds.add(recordInfo.getDocId());
              if (validDocIdsForOldSegment != null) {
                validDocIdsForOldSegment.remove(currentRecordLocation.getDocId());
              }
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
              _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
              continue;
            } else {
              continue;
            }
          }

          // This should not happen because the previously replaced segment should have all keys removed. We still
          // handle it here, and also track the number of keys not properly replaced previously.
          String currentSegmentName = currentSegment.getSegmentName();
          if (currentSegmentName.equals(segmentName)) {
            numKeysInWrongSegment.getAndIncrement();
            if (comparisonResult >= 0) {
              validDocIds.add(recordInfo.getDocId());
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
              _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
              continue;
            } else {
              continue;
            }
          }

          // The current record is in a different segment
          // Update the record location when getting a newer comparison value, or the value is the same as the
          // current value, but the segment has a larger sequence number (the segment is newer than the current
          // segment).
          if (comparisonResult > 0 || (comparisonResult == 0 && LLCSegmentName.isLowLevelConsumerSegmentName(segmentName) && LLCSegmentName.isLowLevelConsumerSegmentName(currentSegmentName)
              && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName.getSequenceNumber(currentSegmentName))) {
            Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentRecordLocation.getDocId());
            validDocIds.add(recordInfo.getDocId());
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
            _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo.getDocId());
          RecordLocationWithSegmentId newRecordLocationWithSegmentId = new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
          _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
        }
      } catch (RocksDBException ex) {
        _logger.warn("Could not update partition metadata in rocksDB for segment {}", segmentName, ex);
      }
    }
    int numKeys = numKeysInWrongSegment.get();
    if (numKeys > 0) {
      _logger.warn("Found {} primary keys in the wrong segment when adding segment: {}", numKeys, segmentName);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, numKeys);
    }
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  @Override
  public void addRecord(MutableSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    int segmentId = getSegmentId(segment);

    try {
      byte[] primaryKey = HashUtils.hashPrimaryKeyAsBytes(recordInfo.getPrimaryKey(), _hashFunction);
      byte[] value = _rocksDB.get(_readOptions, primaryKey);

      if (value != null) {
        // Existing primary key
        RecordLocationWithSegmentId currentRecordLocation = new RecordLocationWithSegmentId(value);

        // Update the record location when the new comparison value is greater than or equal to the current value.
        // Update the record location when there is a tie to keep the newer record.
        if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
          IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegment());
          int currentDocId = currentRecordLocation.getDocId();
          if (segment == currentSegment) {
            validDocIds.replace(currentDocId, recordInfo.getDocId());
          } else {
            Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
            validDocIds.add(recordInfo.getDocId());
          }
          RecordLocationWithSegmentId newRecordLocationWithSegmentId = new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
          _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
        }
      } else {
        // New primary key
        validDocIds.add(recordInfo.getDocId());
        RecordLocationWithSegmentId newRecordLocationWithSegmentId = new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), (long) recordInfo.getComparisonValue());
        _rocksDB.put(_writeOptions, primaryKey, newRecordLocationWithSegmentId.toBytes());
      }
    } catch (RocksDBException ex) {
      _logger.warn("Could not update partition metadata in rocksDB for segment {}", segment.getSegmentName(), ex);
    }

    // Update metrics
//    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
//        _primaryKeyToRecordLocationMap.size());
  }

  /**
   * Replaces the upsert metadata for the old segment with the new immutable segment.
   */
  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    replaceSegment(segment, null, null, oldSegment);
  }

  @VisibleForTesting
  void replaceSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable Iterator<RecordInfo> recordInfoIterator, IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
//    _logger.info("Replacing {} segment: {}, current primary key count: {}",
//        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName,
//        _primaryKeyToRecordLocationMap.size());

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIdsForOldSegment =
          oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (segment instanceof EmptyIndexSegment) {
        _logger.info("Skip adding empty segment: {}", segmentName);
      } else {
        Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
            "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
            _tableNameWithType);
        if (validDocIds == null) {
          validDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        if (recordInfoIterator == null) {
          recordInfoIterator = getRecordInfoIterator(segment);
        }
        addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, recordInfoIterator, oldSegment,
            validDocIdsForOldSegment);
      }

      if (validDocIdsForOldSegment != null && !validDocIdsForOldSegment.isEmpty()) {
        int numKeysNotReplaced = validDocIdsForOldSegment.getCardinality();
        if (_partialUpsertHandler != null) {
          // For partial-upsert table, because we do not restore the original record location when removing the primary
          // keys not replaced, it can potentially cause inconsistency between replicas. This can happen when a
          // consuming segment is replaced by a committed segment that is consumed from a different server with
          // different records (some stream consumer cannot guarantee consuming the messages in the same order).
          _logger.warn("Found {} primary keys not replaced when replacing segment: {} for partial-upsert table. This "
              + "can potentially cause inconsistency between replicas", numKeysNotReplaced, segmentName);
          _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED,
              numKeysNotReplaced);
        } else {
          _logger.info("Found {} primary keys not replaced when replacing segment: {}", numKeysNotReplaced,
              segmentName);
        }
        removeSegment(oldSegment, validDocIdsForOldSegment);
      }
    } finally {
      segmentLock.unlock();
    }

    if (!(oldSegment instanceof EmptyIndexSegment)) {
      _replacedSegments.add(oldSegment);
    }

    // Update metrics
//    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
//    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
//        numPrimaryKeys);

//    _logger.info("Finished replacing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  /**
   * Removes the upsert metadata for the given segment.
   */
  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
//    _logger.info("Removing {} segment: {}, current primary key count: {}",
//        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName,
//        _primaryKeyToRecordLocationMap.size());

    if (_replacedSegments.remove(segment)) {
      _logger.info("Skip removing replaced segment: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIds =
          segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (validDocIds == null || validDocIds.isEmpty()) {
        _logger.info("Skip removing segment without valid docs: {}", segmentName);
        return;
      }

      _logger.info("Removing {} primary keys for segment: {}", validDocIds.getCardinality(), segmentName);
      removeSegment(segment, validDocIds);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
//    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
//    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
//        numPrimaryKeys);
//
//    _logger.info("Finished removing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  private void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    assert !validDocIds.isEmpty();
    PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
    PeekableIntIterator iterator = validDocIds.getIntIterator();
    int segmentId = getSegmentId(segment);
    while (iterator.hasNext()) {
      try {
        int docId = iterator.next();
        getPrimaryKey(segment, docId, primaryKey);

        byte[] primaryKeyBytes = HashUtils.hashPrimaryKeyAsBytes(primaryKey, _hashFunction);
        byte[] value = _rocksDB.get(_readOptions, primaryKeyBytes);

        if (value != null) {
          // Existing primary key
          RecordLocationWithSegmentId recordLocation = new RecordLocationWithSegmentId(value);
          if (recordLocation.getSegment() == segmentId) {
            _rocksDB.delete(_writeOptions, primaryKeyBytes);
          }
        }
      } catch (RocksDBException ex) {

      }
    }
  }

  /**
   * Returns the merged record when partial-upsert is enabled.
   */
  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    // Directly return the record when partial-upsert is not enabled
    if (_partialUpsertHandler == null) {
      return record;
    }

    byte[] primaryKeyBytes = HashUtils.hashPrimaryKeyAsBytes(recordInfo.getPrimaryKey(), _hashFunction);
    byte[] value = null;
    try {
      value = _rocksDB.get(_readOptions, primaryKeyBytes);
    } catch (RocksDBException e) {
      _logger.warn("Cannot get record for key: {}", recordInfo.getPrimaryKey(), e);
    }

    if (value != null) {
      RecordLocationWithSegmentId currentRecordLocation = new RecordLocationWithSegmentId(value);

      // Existing primary key
      if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
        _reuse.clear();
        int segmentId = currentRecordLocation.getSegment();
        IndexSegment indexSegment = (IndexSegment) _segmentIdToSegmentMap.get(segmentId);
        if(indexSegment != null) {
          GenericRow previousRecord = indexSegment.getRecord(currentRecordLocation.getDocId(), _reuse);
          return _partialUpsertHandler.merge(previousRecord, record);
        } else {
          _logger.warn("Segment not found in upsert metadata for segmentId: {}. This should not happen and can lead to inconsistencies in partial upsert", segmentId);
          return record;
        }
      } else {
        _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, 1L);
        _numOutOfOrderEvents++;
        long currentTimeNs = System.nanoTime();
        if (currentTimeNs - _lastOutOfOrderEventReportTimeNs > OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS) {
          _logger.warn("Skipped {} out-of-order events for partial-upsert table (the last event has current comparison "
                  + "value: {}, record comparison value: {})", _numOutOfOrderEvents,
              currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
          _lastOutOfOrderEventReportTimeNs = currentTimeNs;
          _numOutOfOrderEvents = 0;
        }
        return record;
      }
    } else {
      // New primary key
      return record;
    }
  }

  private int getSegmentId(IndexSegment segment) {
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });
    return segmentId;
  }

  class RecordLocationWithSegmentId {
    private final int _segment;
    private final int _docId;
    /** value used to denote the order */
    private final Comparable _comparisonValue;

    public RecordLocationWithSegmentId(int indexSegment, int docId, Comparable comparisonValue) {
      _segment = indexSegment;
      _docId = docId;
      _comparisonValue = comparisonValue;
    }

    public RecordLocationWithSegmentId(byte[] serializedRecordLocation) {
      ByteBuffer buffer = ByteBuffer.wrap(serializedRecordLocation);
      _segment = buffer.getInt();
      _docId = buffer.getInt();
      switch(_comparisonColumn.getDataType()) {
        case INT:
          _comparisonValue = buffer.getInt();
          break;
        case LONG:
          _comparisonValue = buffer.getLong();
          break;
        case FLOAT:
          _comparisonValue = buffer.getFloat();
          break;
        case DOUBLE:
          _comparisonValue = buffer.getDouble();
          break;
        case TIMESTAMP:
          _comparisonValue = buffer.getLong();
          break;
        case STRING:
          _comparisonValue = new String(buffer.slice().array());
          break;
        default:
          _comparisonValue = null;
          _logger.error("Comparison column datatype {} not supported in serialization", _comparisonColumn.getDataType());
      }
    }

    public int getSegment() {
      return _segment;
    }

    public int getDocId() {
      return _docId;
    }

    public Comparable getComparisonValue() {
      return _comparisonValue;
    }

    public byte[] toBytes() {
      ByteBuffer buffer = ByteBuffer.allocate(256);
      buffer.putInt(_segment);
      buffer.putInt(_docId);
      switch(_comparisonColumn.getDataType()) {
        case INT:
          buffer.putInt((Integer) _comparisonValue);
          break;
        case LONG:
          buffer.putLong((Long) _comparisonValue);
          break;
        case FLOAT:
          buffer.putFloat((Float) _comparisonValue);
          break;
        case DOUBLE:
          buffer.putDouble((Double) _comparisonValue);
          break;
        case TIMESTAMP:
          buffer.putLong((Long) _comparisonValue);
          break;
        case STRING:
          buffer.put(((String) _comparisonValue).getBytes(StandardCharsets.UTF_8));
          break;
        default:
          _logger.warn("Comparison column datatype {} not supported in serialization", _comparisonColumn.getDataType());
      }
      return buffer.array();
    }
  }

}

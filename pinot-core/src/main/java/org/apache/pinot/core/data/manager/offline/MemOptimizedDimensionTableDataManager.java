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
package org.apache.pinot.core.data.manager.offline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


/**
 * Dimension Table is a special type of OFFLINE table which is assigned to all servers
 * in a tenant and is used to execute a LOOKUP Transform Function. DimensionTableDataManager
 * loads the contents into a HashMap for faster access thus the size should be small
 * enough to easily fit in memory.
 *
 * DimensionTableDataManager uses Registry of Singletons pattern to store one instance per table
 * which can be accessed via {@link #getInstanceByTableName} static method.
 */
@ThreadSafe
public class MemOptimizedDimensionTableDataManager extends BaseDimensionTableDataManager {

  // Storing singletons per table in a map
  private static final Map<String, MemOptimizedDimensionTableDataManager> INSTANCES = new ConcurrentHashMap<>();

  private MemOptimizedDimensionTableDataManager() {
  }

  /**
   * `createInstanceByTableName` should only be used by the {@link TableDataManagerProvider} and the returned
   * instance should be properly initialized via {@link #init} method before using.
   */
  public static MemOptimizedDimensionTableDataManager createInstanceByTableName(String tableNameWithType) {
    return INSTANCES.computeIfAbsent(tableNameWithType, k -> new MemOptimizedDimensionTableDataManager());
  }

  @VisibleForTesting
  public static MemOptimizedDimensionTableDataManager registerDimensionTable(String tableNameWithType,
      MemOptimizedDimensionTableDataManager instance) {
    return INSTANCES.computeIfAbsent(tableNameWithType, k -> instance);
  }

  public static MemOptimizedDimensionTableDataManager getInstanceByTableName(String tableNameWithType) {
    return INSTANCES.get(tableNameWithType);
  }

  private static final AtomicReferenceFieldUpdater<MemOptimizedDimensionTableDataManager, MemoryOptimizedDimensionTable> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(MemOptimizedDimensionTableDataManager.class, MemoryOptimizedDimensionTable.class, "_dimensionTable");

  private volatile MemoryOptimizedDimensionTable _dimensionTable;

  @Override
  protected void doInit() {
    super.doInit();
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);
    _dimensionTable = new MemoryOptimizedDimensionTable(schema, primaryKeyColumns);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    super.addSegment(immutableSegment);
    String segmentName = immutableSegment.getSegmentName();
    try {
      loadLookupTable();
      _logger.info("Successfully loaded lookup table: {} after adding segment: {}", _tableNameWithType, segmentName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after adding segment: %s", _tableNameWithType,
              segmentName), e);
    }
  }

  @Override
  public void removeSegment(String segmentName) {
    super.removeSegment(segmentName);
    try {
      loadLookupTable();
      _logger.info("Successfully loaded lookup table: {} after removing segment: {}", _tableNameWithType, segmentName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while loading lookup table: %s after removing segment: %s",
              _tableNameWithType, segmentName), e);
    }
  }

  /**
   * `loadLookupTable()` reads contents of the DimensionTable into _lookupTable HashMap for fast lookup.
   */
  @Override
  public void loadLookupTable() {
    MemoryOptimizedDimensionTable snapshot;
    MemoryOptimizedDimensionTable replacement;
    do {
      snapshot = _dimensionTable;
      replacement = createDimensionTable();
    } while (!UPDATER.compareAndSet(this, snapshot, replacement));
  }

  private MemoryOptimizedDimensionTable createDimensionTable() {
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for dimension table: %s", _tableNameWithType);
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkState(CollectionUtils.isNotEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dimension table: %s", _tableNameWithType);

    Map<PrimaryKey, LookupRecordLocation> lookupTable = new HashMap<>();
    List<SegmentDataManager> segmentManagers = acquireAllSegments();
    try {
      for (SegmentDataManager segmentManager : segmentManagers) {
        IndexSegment indexSegment = segmentManager.getSegment();
        int numTotalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
        if (numTotalDocs > 0) {
          try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
            recordReader.init(indexSegment);
            for (int i = 0; i < numTotalDocs; i++) {
              GenericRow row = new GenericRow();
              recordReader.getRecord(i, row);
              lookupTable.put(row.getPrimaryKey(primaryKeyColumns), new LookupRecordLocation(indexSegment, i));
            }
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while reading records from segment: " + indexSegment.getSegmentName());
          }
        }
      }
      return new MemoryOptimizedDimensionTable(schema, primaryKeyColumns, lookupTable);
    } finally {
      for (SegmentDataManager segmentManager : segmentManagers) {
        releaseSegment(segmentManager);
      }
    }
  }

  @Override
  public boolean isPopulated() {
    return !_dimensionTable.isEmpty();
  }

  @Override
  public GenericRow lookupRowByPrimaryKey(PrimaryKey pk) {
    return _dimensionTable.get(pk);
  }

  @Override
  public FieldSpec getColumnFieldSpec(String columnName) {
    return _dimensionTable.getFieldSpecFor(columnName);
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _dimensionTable.getPrimaryKeyColumns();
  }
}

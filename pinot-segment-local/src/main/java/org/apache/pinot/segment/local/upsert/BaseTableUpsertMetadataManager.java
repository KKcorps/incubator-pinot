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

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


@ThreadSafe
public abstract class BaseTableUpsertMetadataManager implements TableUpsertMetadataManager {
  protected String _tableNameWithType;
  protected List<String> _primaryKeyColumns;
  protected List<String> _comparisonColumns;
  protected String _deleteRecordColumn;
  protected HashFunction _hashFunction;
  protected PartialUpsertHandler _partialUpsertHandler;
  protected boolean _enableSnapshot;
  protected ServerMetrics _serverMetrics;
  protected boolean _preloadSegments;
  protected Map<String, Boolean> _preloadedSegmentsMap = new HashMap<>(); // will be used to track preloaded segments and return message to helix on addSegment call

  protected HelixManager _helixManager;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager,
      ServerMetrics serverMetrics, HelixManager helixManager, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableConfig.getTableName();

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    _primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(_primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    _comparisonColumns = upsertConfig.getComparisonColumns();
    if (_comparisonColumns == null) {
      _comparisonColumns = Collections.singletonList(tableConfig.getValidationConfig().getTimeColumnName());
    }

    _deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    _hashFunction = upsertConfig.getHashFunction();

    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      _partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig.getDefaultPartialUpsertStrategy(),
              _comparisonColumns);
    }

    _enableSnapshot = upsertConfig.isEnableSnapshot();
    _serverMetrics = serverMetrics;
    _helixManager = helixManager;
    _propertyStore = propertyStore;

    _preloadSegments = upsertConfig.isEnableSnapshot(); //TODO: should be configurable
    if(_preloadSegments) {
      preloadSegments(tableDataManager, tableConfig, schema);
    }
  }

  public void preloadSegments(TableDataManager tableDataManager, TableConfig tableConfig, Schema schema) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, _tableNameWithType);

    List<String> segmentsWithSnapshots = new LinkedList<>();
    List<String> segmentsWithoutSnapshots = new LinkedList<>();

    //TODO: should be multi-threaded (done using executor service maybe?)
    for (String segmentName : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
      System.out.println("partitionName: " + segmentName);
      System.out.println("instanceId: " + tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getInstanceId());
      String state = instanceStateMap.get(tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getInstanceId());
      String tableNameWithType = _tableNameWithType;
      if (Objects.equals(state, "ONLINE")) {
//        LOGGER.info("Adding or replacing segment: {} for table: {}", segmentName, tableNameWithType);

        // But if table mgr is not created or the segment is not loaded yet, the localMetadata
        // is set to null. Then, addOrReplaceSegment method will load the segment accordingly.
        SegmentMetadata localMetadata = getSegmentMetadata(tableDataManager, tableNameWithType, segmentName);

        if (getValidDocIdsSnapshotFile(localMetadata).exists()) {
          segmentsWithSnapshots.add(segmentName);
        } else {
          segmentsWithoutSnapshots.add(segmentName);
        }
      }
    }

    for (String segmentName : segmentsWithSnapshots) {
      try {
        loadSegmentFromDisk(tableDataManager, tableConfig, segmentName);
        _preloadedSegmentsMap.put(segmentName, true);
      } catch (Exception e) {
        //TODO: log error
      }
    }

    //TODO: Missing step - Change from write only mode to read-write mode

    for (String segmentName : segmentsWithoutSnapshots) {
      try {
        loadSegmentFromDisk(tableDataManager, tableConfig, segmentName);
        _preloadedSegmentsMap.put(segmentName, true);
      } catch (Exception e) {
       //TODO: log error
      }
    }
  }

  //TODO: duplicate method from ImmutableSegmentImpl
  private File getValidDocIdsSnapshotFile(SegmentMetadata segmentMetadata) {
    return new File(SegmentDirectoryPaths.findSegmentDirectory(segmentMetadata.getIndexDir()),
        V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
  }


  private void loadSegmentFromDisk(TableDataManager tableDataManager, TableConfig tableConfig, String segmentName)
      throws Exception {
    String tableNameWithType = _tableNameWithType;
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableConfig);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s", segmentName, tableNameWithType);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // But if table mgr is not created or the segment is not loaded yet, the localMetadata
      // is set to null. Then, addOrReplaceSegment method will load the segment accordingly.
      SegmentMetadata localMetadata = getSegmentMetadata(tableDataManager, tableNameWithType, segmentName);
      tableDataManager.addOrReplaceSegment(segmentName, new IndexLoadingConfig(tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig(), tableConfig, schema),
          zkMetadata, localMetadata);
    } finally {
      segmentLock.unlock();
    }
  }


  public SegmentMetadata getSegmentMetadata(TableDataManager tableDataManager, String tableNameWithType, String segmentName) {
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      return null;
    }
    try {
      return segmentDataManager.getSegment().getSegmentMetadata();
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }
}

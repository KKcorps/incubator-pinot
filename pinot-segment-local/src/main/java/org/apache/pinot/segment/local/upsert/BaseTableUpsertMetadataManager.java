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
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private TableDataManager _tableDataManager;
  public static final Logger LOGGER = LoggerFactory.getLogger(BaseTableUpsertMetadataManager.class);

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
    _tableDataManager = tableDataManager;

    _preloadSegments = upsertConfig.isEnableSnapshot(); //TODO: should be configurable
    if(_preloadSegments) {
      preloadSegments(tableDataManager, tableConfig, schema);
    }
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }

  public void preloadSegments(TableDataManager tableDataManager, TableConfig tableConfig, Schema schema) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, _tableNameWithType);

    List<String> segmentsWithSnapshots = new LinkedList<>();
    List<String> segmentsWithoutSnapshots = new LinkedList<>();

    for (String segmentName : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
      System.out.println("partitionName: " + segmentName);
      System.out.println("instanceId: " + tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getInstanceId());
      String state = instanceStateMap.get(tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getInstanceId());
      String tableNameWithType = _tableNameWithType;
      if (Objects.equals(state, "ONLINE")) {
        LOGGER.info("Adding or replacing segment: {} for table: {}", segmentName, tableNameWithType);

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

    //TODO: should be multi-threaded (done using executor service maybe?)
    for (String segmentName : segmentsWithSnapshots) {
      try {
        loadSegmentFromDisk(tableConfig, segmentName);
        _preloadedSegmentsMap.put(segmentName, true);
      } catch (Exception e) {
        LOGGER.warn("Failed to load segment: {} for table: {}", segmentName, _tableNameWithType, e);
      }
    }

    //TODO: Missing step - Change from write only mode to read-write mode

    for (String segmentName : segmentsWithoutSnapshots) {
      try {
        loadSegmentFromDisk(tableConfig, segmentName);
        _preloadedSegmentsMap.put(segmentName, true);
      } catch (Exception e) {
        LOGGER.warn("Failed to load segment: {} for table: {}", segmentName, _tableNameWithType, e);
      }
    }
  }

  //TODO: duplicate methods from ImmutableSegmentImpl and TableDataManager. Move to a common place
  private File getValidDocIdsSnapshotFile(SegmentMetadata segmentMetadata) {
    return new File(SegmentDirectoryPaths.findSegmentDirectory(segmentMetadata.getIndexDir()),
        V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
  }


  private void loadSegmentFromDisk(TableConfig tableConfig, String segmentName)
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
      SegmentMetadata localMetadata = getSegmentMetadata(_tableDataManager, tableNameWithType, segmentName);
      IndexLoadingConfig indexLoadingConfig =
          new IndexLoadingConfig(_tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig(), tableConfig, schema);
      _tableDataManager.addOrReplaceSegment(segmentName, new IndexLoadingConfig(_tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig(), tableConfig, schema),
          zkMetadata, localMetadata);

      File indexDir = _tableDataManager.getTableDataDir();
      SegmentDirectory segmentDirectory =
          tryInitSegmentDirectory(segmentName, String.valueOf(zkMetadata.getCrc()), indexLoadingConfig);

      if (!ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig, schema)) {
        LOGGER.info("Segment: {} of table: {} is consistent with latest table config and schema", segmentName,
            _tableNameWithType);
      } else {
        LOGGER.info("Segment: {} of table: {} needs reprocess to reflect latest table config and schema", segmentName,
            _tableNameWithType);
        segmentDirectory.copyTo(indexDir);
        // Close the stale SegmentDirectory object and recreate it with reprocessed segment.
        closeSegmentDirectoryQuietly(segmentDirectory);
        ImmutableSegmentLoader.preprocess(indexDir, indexLoadingConfig, schema);
        segmentDirectory = initSegmentDirectory(segmentName, String.valueOf(zkMetadata.getCrc()), indexLoadingConfig);
      }
      ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema);
      _tableDataManager.addSegment(segment);
    } finally {
      segmentLock.unlock();
    }
  }

  private SegmentDirectory tryInitSegmentDirectory(String segmentName, String segmentCrc,
      IndexLoadingConfig indexLoadingConfig) {
    try {
      return initSegmentDirectory(segmentName, segmentCrc, indexLoadingConfig);
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize SegmentDirectory for segment: {} of table: {} with error: {}", segmentName,
          _tableNameWithType, e.getMessage());
      return null;
    }
  }

  private SegmentDirectory initSegmentDirectory(String segmentName, String segmentCrc,
      IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    SegmentDirectoryLoaderContext loaderContext =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(indexLoadingConfig.getTableConfig())
            .setSchema(indexLoadingConfig.getSchema()).setInstanceId(indexLoadingConfig.getInstanceId())
            .setTableDataDir(indexLoadingConfig.getTableDataDir()).setSegmentName(segmentName).setSegmentCrc(segmentCrc)
            .setSegmentTier(indexLoadingConfig.getSegmentTier())
            .setInstanceTierConfigs(indexLoadingConfig.getInstanceTierConfigs())
            .setSegmentDirectoryConfigs(indexLoadingConfig.getSegmentDirectoryConfigs()).build();
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    File indexDir =
        getSegmentDataDir(segmentName, indexLoadingConfig.getSegmentTier(), indexLoadingConfig.getTableConfig());
    return segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
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

  @VisibleForTesting
  File getSegmentDataDir(String segmentName) {
    return new File(_tableDataManager.getTableDataDir(), segmentName);
  }

  @VisibleForTesting
  File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig) {
    if (segmentTier == null) {
      return getSegmentDataDir(segmentName);
    }
    String tierDataDir =
        TierConfigUtils.getDataDirForTier(tableConfig, segmentTier, _tableDataManager.getTableDataManagerConfig().getInstanceTierConfigs());
    if (StringUtils.isEmpty(tierDataDir)) {
      return getSegmentDataDir(segmentName);
    }
    File tierTableDataDir = new File(tierDataDir, _tableNameWithType);
    return new File(tierTableDataDir, segmentName);
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }
}

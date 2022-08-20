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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.upsert.metastore.rocksdb.RocksDBStore;
import org.apache.pinot.segment.local.upsert.metastore.rocksdb.RocksDBUtils;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class RocksDbTableUpsertMetadataManager implements TableUpsertMetadataManager {
  private String _tableNameWithType;
  private List<String> _primaryKeyColumns;
  private FieldSpec _comparisonColumn;
  private File _dataDir;
  private UpsertConfig _upsertConfig;

  private HashFunction _hashFunction;
  private PartialUpsertHandler _partialUpsertHandler;
  private ServerMetrics _serverMetrics;

  private RocksDBStore _rocksDB;

  private final Map<Integer, RocksDBStorePartitionUpsertMetadataManager> _partitionMetadataManagerMap =
      new ConcurrentHashMap<>();

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager,
      ServerMetrics serverMetrics) {
    _tableNameWithType = tableConfig.getTableName();

    _upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(_upsertConfig != null && _upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    _primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(_primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    String comparisonColumnName = _upsertConfig.getComparisonColumn();
    if (comparisonColumnName == null) {
      comparisonColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    }
    _comparisonColumn = schema.getFieldSpecFor(comparisonColumnName);

    _hashFunction = _upsertConfig.getHashFunction();

    if (_upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = _upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      _partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, _upsertConfig.getDefaultPartialUpsertStrategy(),
              _comparisonColumn.getName());
    }

    _dataDir = tableDataManager.getTableDataDir();
    _serverMetrics = serverMetrics;

    Map<String, String> configs = _upsertConfig.getMetadataManagerConfigs();
    configs.put(RocksDBUtils.DATA_DIR, _dataDir.getAbsolutePath());
    _rocksDB = new RocksDBStore();
    _rocksDB.init(configs);
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }

  @Override
  public RocksDBStorePartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId, k -> {
      try {
        return new RocksDBStorePartitionUpsertMetadataManager(_tableNameWithType, k,
            _primaryKeyColumns, _comparisonColumn, _hashFunction, _partialUpsertHandler,
            _serverMetrics, _rocksDB);
      } catch (Exception e) {
          //log error
      }
      return null;
    });
  }

  @Override
  public void close()
      throws IOException {
    _rocksDB.close();
  }
}

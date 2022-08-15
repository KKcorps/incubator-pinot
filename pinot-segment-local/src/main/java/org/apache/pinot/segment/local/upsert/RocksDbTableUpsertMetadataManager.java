package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
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

  private final Map<Integer, RocksDbPartitionUpsertMetadataManager> _partitionMetadataManagerMap =
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
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }

  @Override
  public RocksDbPartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId, k -> {
      try {
        return new RocksDbPartitionUpsertMetadataManager(_tableNameWithType, k, _primaryKeyColumns, _comparisonColumn,
            _hashFunction, _partialUpsertHandler, _serverMetrics, _dataDir, _upsertConfig);
      } catch (Exception e) {
          //log error
      }
      return null;
    });
  }
}

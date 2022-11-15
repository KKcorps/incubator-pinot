package org.apache.pinot.core.data.manager.offline;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.spi.config.table.DimensionTableConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DimensionTableDataManagerFactory {

  private DimensionTableDataManagerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionTableDataManagerFactory.class);
  private static final Map<String, BaseDimensionTableDataManager> _dimensionTableDataManagerMap =
      new ConcurrentHashMap<>();

  public static BaseDimensionTableDataManager create(ZkHelixPropertyStore<ZNRecord> propertyStore, TableDataManagerConfig tableDataManagerConfig) {
    String tableNameWithType = tableDataManagerConfig.getTableName();
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
    DimensionTableConfig dimensionTableConfig = tableConfig.getDimensionTableConfig();
    Preconditions.checkArgument(dimensionTableConfig != null, "Must provide upsert config for table: %s", tableNameWithType);

    BaseDimensionTableDataManager dimensionTableDataManager;
    if (dimensionTableConfig.isOptimizeMemory()) {
      dimensionTableDataManager = MemOptimizedDimensionTableDataManager.createInstanceByTableName(tableConfig.getTableName());
    } else {
      dimensionTableDataManager =  DimensionTableDataManager.createInstanceByTableName(tableConfig.getTableName());
    }

    _dimensionTableDataManagerMap.compute(tableNameWithType, (t, d) -> dimensionTableDataManager);

    return dimensionTableDataManager;
  }

  public static BaseDimensionTableDataManager getOrCreateDimensionTableDataManager(ZkHelixPropertyStore<ZNRecord> propertyStore, TableDataManagerConfig tableDataManagerConfig) {
    String tableNameWithType = tableDataManagerConfig.getTableName();
    return _dimensionTableDataManagerMap.computeIfAbsent(tableNameWithType,
        k -> create(propertyStore, tableDataManagerConfig));
  }

  @Nullable
  public static BaseDimensionTableDataManager getDimensionTableDataManager(String tableNameWithType) {
    return _dimensionTableDataManagerMap.get(tableNameWithType);
  }

  public void close()
      throws IOException {

  }

}

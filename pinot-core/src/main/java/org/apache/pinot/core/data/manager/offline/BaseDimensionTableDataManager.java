package org.apache.pinot.core.data.manager.offline;

import java.util.List;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public abstract class BaseDimensionTableDataManager extends OfflineTableDataManager {

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

  public abstract void loadLookupTable();

  public abstract boolean isPopulated();

  public abstract GenericRow lookupRowByPrimaryKey(PrimaryKey pk);

  public abstract FieldSpec getColumnFieldSpec(String columnName);

  public abstract List<String> getPrimaryKeyColumns();
}

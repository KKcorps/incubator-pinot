package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class DimensionTableConfig extends BaseJsonConfig {
  private final boolean _optimizeMemory;

  @JsonCreator
  public DimensionTableConfig(@JsonProperty(value = "optimizeMemory", required = true) boolean optimizeMemory) {
    _optimizeMemory = optimizeMemory;
  }

  public boolean isOptimizeMemory() {
      return _optimizeMemory;
  }
}

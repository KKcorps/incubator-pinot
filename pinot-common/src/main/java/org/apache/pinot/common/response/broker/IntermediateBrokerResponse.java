package org.apache.pinot.common.response.broker;

import java.util.Map;


public class IntermediateBrokerResponse {
  private ResultTable _resultTable;
  private Map<String, String> _metadata;

  public IntermediateBrokerResponse() {
  }

  public IntermediateBrokerResponse(ResultTable resultTable, Map<String, String> metadata) {
    _resultTable = resultTable;
    _metadata = metadata;
  }

  public ResultTable getResultTable() {
    return _resultTable;
  }

  public void setResultTable(ResultTable resultTable) {
    _resultTable = resultTable;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }
}

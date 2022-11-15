package org.apache.pinot.core.data.manager.offline;

import org.apache.pinot.segment.spi.IndexSegment;


public class LookupRecordLocation {
  private final IndexSegment _segment;
  private final int _docId;

  public LookupRecordLocation(IndexSegment segment, int docId) {
    _segment = segment;
    _docId = docId;
  }

  public IndexSegment getSegment() {
    return _segment;
  }

  public int getDocId() {
    return _docId;
  }
}

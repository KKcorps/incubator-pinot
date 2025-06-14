package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Response object for deleteSegmentsFromSequenceNum API.
 */
public class DeleteSegmentsFromSequenceNumResponse {
  private final List<String> _segments;
  private final boolean _dryRun;

  @JsonCreator
  public DeleteSegmentsFromSequenceNumResponse(
      @JsonProperty("segments") List<String> segments,
      @JsonProperty("dryRun") boolean dryRun) {
    _segments = segments;
    _dryRun = dryRun;
  }

  @JsonProperty("segments")
  public List<String> getSegments() {
    return _segments;
  }

  @JsonProperty("dryRun")
  public boolean isDryRun() {
    return _dryRun;
  }
}


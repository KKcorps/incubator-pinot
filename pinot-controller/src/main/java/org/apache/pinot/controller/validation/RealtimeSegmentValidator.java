package org.apache.pinot.controller.validation;

import java.util.ArrayList;
import java.util.List;
import com.google.common.annotations.VisibleForTesting;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that performs segment level validation for realtime tables.
 */
public class RealtimeSegmentValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentValidator.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;
  private final ControllerMetrics _controllerMetrics;

  public RealtimeSegmentValidator(PinotHelixResourceManager pinotHelixResourceManager,
      PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager, ValidationMetrics validationMetrics,
      ControllerMetrics controllerMetrics) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _controllerMetrics = controllerMetrics;
  }

  /**
   * Run segment level validation for the given realtime table.
   */
  public void validate(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();
    List<SegmentZKMetadata> segmentsZKMetadata =
        _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName);

    // Delete tmp segments
    if (_llcRealtimeSegmentManager.isTmpSegmentAsyncDeletionEnabled()) {
      try {
        long startTimeMs = System.currentTimeMillis();
        int numDeletedTmpSegments =
            _llcRealtimeSegmentManager.deleteTmpSegments(realtimeTableName, segmentsZKMetadata);
        LOGGER.info("Deleted {} tmp segments for table: {} in {}ms", numDeletedTmpSegments, realtimeTableName,
            System.currentTimeMillis() - startTimeMs);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.DELETED_TMP_SEGMENT_COUNT,
            numDeletedTmpSegments);
      } catch (Exception e) {
        LOGGER.error("Failed to delete tmp segments for table: {}", realtimeTableName, e);
      }
    }

    // Update the total document count gauge
    _validationMetrics.updateTotalDocumentCountGauge(realtimeTableName,
        computeTotalDocumentCount(segmentsZKMetadata));

    // Ensures all segments in COMMITTING state are properly tracked in ZooKeeper.
    // Acts as a recovery mechanism for segments that may have failed to register during start of commit protocol.
    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      syncCommittingSegmentsFromMetadata(realtimeTableName, segmentsZKMetadata);
    }

    // Check missing segments and upload them to the deep store
    if (_llcRealtimeSegmentManager.isDeepStoreLLCSegmentUploadRetryEnabled()) {
      _llcRealtimeSegmentManager.uploadToDeepStoreIfMissing(tableConfig, segmentsZKMetadata);
    }
  }

  private void syncCommittingSegmentsFromMetadata(String realtimeTableName,
      List<SegmentZKMetadata> segmentsZKMetadata) {
    List<String> committingSegments = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (CommonConstants.Segment.Realtime.Status.COMMITTING.equals(segmentZKMetadata.getStatus())) {
        committingSegments.add(segmentZKMetadata.getSegmentName());
      }
    }
    LOGGER.info("Adding committing segments to ZK: {}", committingSegments);
    if (!_llcRealtimeSegmentManager.syncCommittingSegments(realtimeTableName, committingSegments)) {
      LOGGER.error("Failed to add committing segments for table: {}", realtimeTableName);
    }
  }

  @VisibleForTesting
  public static long computeTotalDocumentCount(List<SegmentZKMetadata> segmentsZKMetadata) {
    long numTotalDocs = 0;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      numTotalDocs += segmentZKMetadata.getTotalDocs();
    }
    return numTotalDocs;
  }
}

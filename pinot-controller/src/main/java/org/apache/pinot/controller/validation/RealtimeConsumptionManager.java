package org.apache.pinot.controller.validation;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Properties;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to enforce pause state based on resource utilization and storage quota,
 * and recover realtime segments stuck in ERROR state.
 */
public class RealtimeConsumptionManager extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumptionManager.class);

  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final StorageQuotaChecker _storageQuotaChecker;
  private final ResourceUtilizationManager _resourceUtilizationManager;
  private final boolean _segmentAutoResetOnError;

  public RealtimeConsumptionManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ControllerMetrics controllerMetrics, StorageQuotaChecker quotaChecker,
      ResourceUtilizationManager resourceUtilizationManager) {
    super("RealtimeConsumptionManager", config.getRealtimeSegmentValidationFrequencyInSeconds(),
        config.getRealtimeSegmentValidationManagerInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _storageQuotaChecker = quotaChecker;
    _resourceUtilizationManager = resourceUtilizationManager;
    _segmentAutoResetOnError = config.isAutoResetErrorSegmentsOnValidationEnabled();
    Preconditions.checkState(config.getRealtimeSegmentValidationFrequencyInSeconds() > 0);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.warn("Failed to find table config for table: {}, skipping pause check", tableNameWithType);
      return;
    }

    enforcePauseState(tableNameWithType, tableConfig);

    boolean isPauselessConsumptionEnabled =
        org.apache.pinot.common.utils.PauselessConsumptionUtils.isPauselessEnabled(tableConfig);
    if (isPauselessConsumptionEnabled) {
      _llcRealtimeSegmentManager.repairSegmentsInErrorStateForPauselessConsumption(tableConfig);
    } else if (_segmentAutoResetOnError) {
      _pinotHelixResourceManager.resetSegments(tableConfig.getTableName(), null, true);
    }
  }

  /**
   * Update the table pause state based on resource and quota validations.
   */
  @VisibleForTesting
  void enforcePauseState(String tableNameWithType, TableConfig tableConfig) {
    PauseStatusDetails pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
    boolean isTablePaused = pauseStatus.getPauseFlag();
    if (isTablePaused && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.ADMINISTRATIVE)) {
      return;
    }

    boolean isResourceUtilizationWithinLimits =
        _resourceUtilizationManager.isResourceUtilizationWithinLimits(tableNameWithType);
    if (!isResourceUtilizationWithinLimits) {
      LOGGER.warn("Resource utilization limit exceeded for table: {}", tableNameWithType);
      _controllerMetrics.setOrUpdateTableGauge(tableNameWithType,
          ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, 1L);
      if (!isTablePaused || !pauseStatus.getReasonCode()
          .equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
        _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType,
            PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, "Resource utilization limit exceeded.");
      }
      return;
    } else if (isTablePaused && pauseStatus.getReasonCode()
        .equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, "Resource utilization within limits");
      pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
      isTablePaused = pauseStatus.getPauseFlag();
    }
    _controllerMetrics.setOrUpdateTableGauge(tableNameWithType,
        ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, 0L);

    boolean isQuotaExceeded = _storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig);
    if (isQuotaExceeded == isTablePaused) {
      return;
    }
    if (isQuotaExceeded) {
      String storageQuota =
          tableConfig.getQuotaConfig() != null ? tableConfig.getQuotaConfig().getStorage() : "NA";
      _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED,
          "Storage quota of " + storageQuota + " exceeded.");
    } else {
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, "Table storage within quota limits");
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    // No-op
  }

  @Override
  public void cleanUpTask() {
    // No-op
  }
}

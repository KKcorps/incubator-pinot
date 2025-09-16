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
package org.apache.pinot.controller.helix.core.minion.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.task.JobConfig;
import org.apache.pinot.common.minion.MinionTaskDryRunResponse;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * The interface <code>PinotTaskGenerator</code> defines the APIs for task generators.
 */
public interface PinotTaskGenerator {

  /**
   * Initializes the task generator.
   */
  void init(ClusterInfoAccessor clusterInfoAccessor);

  /**
   * Returns the task type of the generator.
   */
  String getTaskType();

  /**
   * Generates a list of tasks to schedule based on the given table configs.
   */
  List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs);

  /**
   * Generates a list of adhoc tasks to schedule based on the given table configs and task configs.
   */
  List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception;

  /**
   * Generates a list of task based on the given table configs, it also gets list of existing task configs
   */
  void generateTasks(List<TableConfig> tableConfigs, List<PinotTaskConfig> pinotTaskConfigs) throws Exception;

  /**
   * Returns the timeout in milliseconds for each task, 3600000 (1 hour) by default.
   */
  default long getTaskTimeoutMs() {
    return JobConfig.DEFAULT_TIMEOUT_PER_TASK;
  }

  /**
   * Returns the maximum number of concurrent tasks allowed per instance, 1 by default.
   */
  default int getNumConcurrentTasksPerInstance() {
    return JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  }

  /**
   * Returns the max number of subtasks allowed per task.
   * This overrides individual task level configs like MinionConstants.TABLE_MAX_NUM_TASKS_KEY
   * Number of subtasks directly impacts the performance of the helix leader and thus the controller
   * So limiting the number of subtasks helps to avoid performance issues
   *
   * Usage
   * 1. This method is used by the scheduling framework to limit the number of subtasks across task types
   * 2. This method can also be used by individual task generators to consider the limit while generating subtasks
   */
  default int getMaxAllowedSubTasksPerTask() {
    return MinionConstants.DEFAULT_MINION_MAX_NUM_OF_SUBTASKS_LIMIT;
  }

  /**
   * Returns the maximum number of attempts per task, 1 by default.
   */
  default int getMaxAttemptsPerTask() {
    return MinionConstants.DEFAULT_MAX_ATTEMPTS_PER_TASK;
  }

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes.
   */
  default void nonLeaderCleanUp() {
  }

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes,
   * given a list of tables that the current controller isn't the leader for.
   */
  default void nonLeaderCleanUp(List<String> tableNamesWithType) {
  }

  /**
   * Gets the minionInstanceTag for the tableConfig
   */
  default String getMinionInstanceTag(TableConfig tableConfig) {
    return CommonConstants.Helix.UNTAGGED_MINION_INSTANCE;
  }

  /**
   * Performs task type specific validations for the given task type.
   * @param tableConfig The table configuration that is getting added/updated/validated.
   * @param schema The schema of the table.
   * @param taskConfigs The task type specific task configuration to be validated.
   */
  default void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
  }

  /**
   * Performs a dry run for the task type using the provided table and task configs.
   * Implementations can override this method to add more context such as warnings or
   * task specific metadata to the response.
   */
  default MinionTaskDryRunResponse.TableTaskDryRunResult dryRunTasks(TableConfig tableConfig,
      Map<String, String> taskConfigs) throws Exception {
    return dryRunTasks(tableConfig, taskConfigs, DryRunOptions.DEFAULT);
  }

  /**
   * Performs a dry run for the task type using the provided table and task configs and optional dry run options.
   * Implementations can override this method to add more context such as warnings or
   * task specific metadata to the response.
   */
  default MinionTaskDryRunResponse.TableTaskDryRunResult dryRunTasks(TableConfig tableConfig,
      Map<String, String> taskConfigs, DryRunOptions dryRunOptions) throws Exception {
    Map<String, String> configsToUse = taskConfigs != null ? taskConfigs : Collections.emptyMap();
    List<PinotTaskConfig> pinotTaskConfigs = generateTasks(tableConfig, configsToUse);
    List<MinionTaskDryRunResponse.SubtaskDryRunResult> subTaskResults = new ArrayList<>(pinotTaskConfigs.size());
    for (PinotTaskConfig pinotTaskConfig : pinotTaskConfigs) {
      subTaskResults.add(new MinionTaskDryRunResponse.SubtaskDryRunResult(new HashMap<>(pinotTaskConfig.getConfigs())));
    }
    MinionTaskDryRunResponse.TableTaskDryRunResult dryRunResult =
        new MinionTaskDryRunResponse.TableTaskDryRunResult(tableConfig.getTableName(), subTaskResults);
    dryRunResult.putSummaryValue("taskCount", dryRunResult.getTotalTaskCount());
    if (dryRunOptions.isVerbose()) {
      dryRunResult.putMetadata("verbose", true);
    }
    return dryRunResult;
  }

  /**
   * Encapsulates optional arguments for dry run execution.
   */
  class DryRunOptions {
    public static final DryRunOptions DEFAULT = DryRunOptions.builder().build();

    private final boolean _verbose;
    private final Map<String, Object> _properties;

    private DryRunOptions(boolean verbose, Map<String, Object> properties) {
      _verbose = verbose;
      _properties = properties;
    }

    public boolean isVerbose() {
      return _verbose;
    }

    public Map<String, Object> getProperties() {
      return _properties;
    }

    public Object getProperty(String key) {
      return _properties.get(key);
    }

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private boolean _verbose;
      private final Map<String, Object> _properties = new HashMap<>();

      public Builder setVerbose(boolean verbose) {
        _verbose = verbose;
        return this;
      }

      public Builder putProperty(String key, Object value) {
        if (key != null && value != null) {
          _properties.put(key, value);
        }
        return this;
      }

      public DryRunOptions build() {
        return new DryRunOptions(_verbose, Collections.unmodifiableMap(new HashMap<>(_properties)));
      }
    }
  }
}

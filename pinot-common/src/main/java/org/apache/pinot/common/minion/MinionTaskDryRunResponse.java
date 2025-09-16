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
package org.apache.pinot.common.minion;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the dry run response for a minion task generator.
 */
@JsonPropertyOrder({"taskType", "tableTaskDryRunResults"})
public class MinionTaskDryRunResponse {
  private String _taskType;
  private final List<TableTaskDryRunResult> _tableTaskDryRunResults = new ArrayList<>();
  private final Map<String, Object> _metadata = new LinkedHashMap<>();

  public MinionTaskDryRunResponse() {
  }

  public MinionTaskDryRunResponse(String taskType) {
    _taskType = taskType;
  }

  @JsonProperty("taskType")
  public String getTaskType() {
    return _taskType;
  }

  public void setTaskType(String taskType) {
    _taskType = taskType;
  }

  @JsonProperty("tableTaskDryRunResults")
  public List<TableTaskDryRunResult> getTableTaskDryRunResults() {
    return _tableTaskDryRunResults;
  }

  public void setTableTaskDryRunResults(List<TableTaskDryRunResult> tableTaskDryRunResults) {
    _tableTaskDryRunResults.clear();
    if (tableTaskDryRunResults != null) {
      _tableTaskDryRunResults.addAll(tableTaskDryRunResults);
    }
  }

  public void addTableResult(TableTaskDryRunResult tableTaskDryRunResult) {
    _tableTaskDryRunResults.add(tableTaskDryRunResult);
  }

  /**
   * Adds a metadata entry that will be serialized at the root level of the dry-run response.
   */
  public void putMetadata(String key, Object value) {
    if (key != null && value != null) {
      _metadata.put(key, value);
    }
  }

  @JsonAnySetter
  public void addMetadataEntry(String key, Object value) {
    if (key == null || value == null) {
      return;
    }
    if (!"taskType".equals(key) && !"tableTaskDryRunResults".equals(key)) {
      _metadata.put(key, value);
    }
  }

  @JsonAnyGetter
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getMetadata() {
    return _metadata;
  }

  /**
   * Represents the dry run result for a table.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public static class TableTaskDryRunResult {
    private String _tableNameWithType;
    private final List<SubtaskDryRunResult> _taskDryRunResults = new ArrayList<>();
    private final List<String> _warnings = new ArrayList<>();
    private final Map<String, Object> _summary = new LinkedHashMap<>();
    private final Map<String, Object> _metadata = new LinkedHashMap<>();
    private final Map<String, Object> _sections = new LinkedHashMap<>();
    private Integer _totalTaskCountOverride;

    public TableTaskDryRunResult() {
    }

    public TableTaskDryRunResult(String tableNameWithType, List<SubtaskDryRunResult> taskDryRunResults) {
      _tableNameWithType = tableNameWithType;
      if (taskDryRunResults != null) {
        _taskDryRunResults.addAll(taskDryRunResults);
        _totalTaskCountOverride = _taskDryRunResults.size();
      }
    }

    @JsonIgnore
    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public void setTableNameWithType(String tableNameWithType) {
      _tableNameWithType = tableNameWithType;
      if (!_metadata.containsKey("table")) {
        _metadata.put("table", tableNameWithType);
      }
    }

    @JsonProperty("table")
    public String getTable() {
      Object table = _metadata.get("table");
      if (table != null) {
        return table.toString();
      }
      return _tableNameWithType;
    }

    @JsonProperty("table")
    public void setTable(String table) {
      _metadata.put("table", table);
    }

    @JsonProperty("tasks")
    public List<SubtaskDryRunResult> getTaskDryRunResults() {
      return _taskDryRunResults;
    }

    @JsonProperty("tasks")
    public void setTaskDryRunResults(List<SubtaskDryRunResult> taskDryRunResults) {
      _taskDryRunResults.clear();
      if (taskDryRunResults != null) {
        _taskDryRunResults.addAll(taskDryRunResults);
      }
      if (_totalTaskCountOverride == null) {
        _totalTaskCountOverride = _taskDryRunResults.size();
      }
      if (!_summary.containsKey("taskCount")) {
        _summary.put("taskCount", getTotalTaskCount());
      }
    }

    public void addTask(SubtaskDryRunResult taskDryRunResult) {
      _taskDryRunResults.add(taskDryRunResult);
      if (_totalTaskCountOverride == null) {
        _totalTaskCountOverride = _taskDryRunResults.size();
      }
      _summary.put("taskCount", getTotalTaskCount());
    }

    @JsonProperty("warnings")
    public List<String> getWarnings() {
      return _warnings;
    }

    @JsonProperty("warnings")
    public void setWarnings(List<String> warnings) {
      _warnings.clear();
      if (warnings != null) {
        _warnings.addAll(warnings);
      }
    }

    public void addWarning(String warning) {
      _warnings.add(warning);
    }

    @JsonProperty("totalTaskCount")
    public int getTotalTaskCount() {
      if (_totalTaskCountOverride != null) {
        return _totalTaskCountOverride;
      }
      return _taskDryRunResults.size();
    }

    @JsonProperty("totalTaskCount")
    public void setTotalTaskCountJson(int totalTaskCount) {
      setTotalTaskCount(totalTaskCount);
    }

    public void setTotalTaskCount(int totalTaskCount) {
      _totalTaskCountOverride = totalTaskCount;
      _summary.put("taskCount", totalTaskCount);
    }

    @JsonProperty("summary")
    public Map<String, Object> getSummary() {
      return _summary;
    }

    @JsonProperty("summary")
    public void setSummary(Map<String, Object> summary) {
      _summary.clear();
      if (summary != null) {
        _summary.putAll(summary);
      }
    }

    public void putSummaryValue(String key, Object value) {
      if (key != null && value != null) {
        _summary.put(key, value);
      }
    }

    @JsonIgnore
    public Map<String, Object> getMetadata() {
      return _metadata;
    }

    public void putMetadata(String key, Object value) {
      if (key != null && value != null) {
        _metadata.put(key, value);
      }
    }

    public void putAllMetadata(Map<String, Object> metadata) {
      if (metadata != null) {
        metadata.forEach(this::putMetadata);
      }
    }

    public void putSection(String name, Object payload) {
      if (name != null && payload != null) {
        _sections.put(name, payload);
      }
    }

    public void removeSection(String name) {
      _sections.remove(name);
    }

    @JsonIgnore
    public Map<String, Object> getSections() {
      return _sections;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalFields() {
      Map<String, Object> additional = new LinkedHashMap<>();
      _metadata.forEach((key, value) -> {
        if (!"table".equals(key)) {
          additional.put(key, value);
        }
      });
      additional.putAll(_sections);
      return additional;
    }

    @JsonAnySetter
    public void captureAdditionalField(String key, Object value) {
      if (key == null || value == null) {
        return;
      }
      if ("table".equals(key)) {
        setTable(value.toString());
        return;
      }
      if ("summary".equals(key) || "warnings".equals(key) || "tasks".equals(key)
          || "totalTaskCount".equals(key)) {
        // Handled by dedicated setters
        return;
      }
      if (value instanceof Map || value instanceof List) {
        _sections.put(key, value);
      } else {
        _metadata.put(key, value);
      }
    }
  }

  /**
   * Represents the dry run result for a single subtask.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public static class SubtaskDryRunResult {
    private final Map<String, String> _taskConfig = new LinkedHashMap<>();
    private final List<String> _warnings = new ArrayList<>();

    public SubtaskDryRunResult() {
    }

    public SubtaskDryRunResult(Map<String, String> taskConfig) {
      if (taskConfig != null) {
        _taskConfig.putAll(taskConfig);
      }
    }

    @JsonProperty("taskConfig")
    public Map<String, String> getTaskConfig() {
      return _taskConfig;
    }

    public void setTaskConfig(Map<String, String> taskConfig) {
      _taskConfig.clear();
      if (taskConfig != null) {
        _taskConfig.putAll(taskConfig);
      }
    }

    @JsonProperty("warnings")
    public List<String> getWarnings() {
      return _warnings;
    }

    public void setWarnings(List<String> warnings) {
      _warnings.clear();
      if (warnings != null) {
        _warnings.addAll(warnings);
      }
    }

    public void addWarning(String warning) {
      _warnings.add(warning);
    }
  }
}

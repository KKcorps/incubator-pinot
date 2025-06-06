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
package org.apache.pinot.broker.routing.segmentpartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo.PartitionInfo;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code PartitionDataManager} manages partitions of a table. It manages
 *   1. all the online segments associated with the partition and their allocated servers
 *   2. all the replica of a specific segment.
 * It provides API to query
 *   1. For each partition ID, what are the servers that contains ALL segments belong to this partition ID.
 *   2. For each server, what are all the partition IDs and list of segments of those partition IDs on this server.
 */
public class SegmentPartitionMetadataManager implements SegmentZkMetadataFetchListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPartitionMetadataManager.class);
  private static final int INVALID_PARTITION_ID = -1;
  private static final long INVALID_CREATION_TIME_MS = -1L;

  private final String _tableNameWithType;

  // static content, if anything changes for the following. a rebuild of routing table is needed.
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;

  // cache-able content, only follow changes if onlineSegments list (of ideal-state) is changed.
  private final Map<String, SegmentInfo> _segmentInfoMap = new HashMap<>();

  // computed value based on status change.
  private transient TablePartitionInfo _tablePartitionInfo;
  private transient TablePartitionReplicatedServersInfo _tablePartitionReplicatedServersInfo;

  public SegmentPartitionMetadataManager(String tableNameWithType, String partitionColumn, String partitionFunctionName,
      int numPartitions) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    int numSegments = onlineSegments.size();
    for (int i = 0; i < numSegments; i++) {
      String segment = onlineSegments.get(i);
      ZNRecord znRecord = znRecords.get(i);
      SegmentInfo segmentInfo = new SegmentInfo(getPartitionId(segment, znRecord), getCreationTimeMs(znRecord),
          getOnlineServers(externalView, segment));
      _segmentInfoMap.put(segment, segmentInfo);
    }
    computeAllTablePartitionInfo();
  }

  private int getPartitionId(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo segmentPartitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord);
    if (segmentPartitionInfo == null || segmentPartitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
      return INVALID_PARTITION_ID;
    }
    if (!_partitionColumn.equals(segmentPartitionInfo.getPartitionColumn())) {
      return INVALID_PARTITION_ID;
    }
    PartitionFunction partitionFunction = segmentPartitionInfo.getPartitionFunction();
    if (!_partitionFunctionName.equalsIgnoreCase(partitionFunction.getName())) {
      return INVALID_PARTITION_ID;
    }
    if (_numPartitions != partitionFunction.getNumPartitions()) {
      return INVALID_PARTITION_ID;
    }
    Set<Integer> partitions = segmentPartitionInfo.getPartitions();
    if (partitions.size() != 1) {
      return INVALID_PARTITION_ID;
    }
    return partitions.iterator().next();
  }

  private static long getCreationTimeMs(@Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      return INVALID_CREATION_TIME_MS;
    }
    return SegmentUtils.getSegmentCreationTimeMs(new SegmentZKMetadata(znRecord));
  }

  private static List<String> getOnlineServers(ExternalView externalView, String segment) {
    Map<String, String> instanceStateMap = externalView.getStateMap(segment);
    if (instanceStateMap == null) {
      return Collections.emptyList();
    }
    List<String> onlineServers = new ArrayList<>(instanceStateMap.size());
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      String instanceState = entry.getValue();
      if (instanceState.equals(SegmentStateModel.ONLINE) || instanceState.equals(SegmentStateModel.CONSUMING)) {
        onlineServers.add(entry.getKey());
      }
    }
    return onlineServers;
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // Update segment partition id for the pulled segments
    int numSegments = pulledSegments.size();
    for (int i = 0; i < numSegments; i++) {
      String segment = pulledSegments.get(i);
      ZNRecord znRecord = znRecords.get(i);
      SegmentInfo segmentInfo = new SegmentInfo(getPartitionId(segment, znRecord), getCreationTimeMs(znRecord),
          getOnlineServers(externalView, segment));
      _segmentInfoMap.put(segment, segmentInfo);
    }
    // Update online servers for all online segments
    for (String segment : onlineSegments) {
      SegmentInfo segmentInfo = _segmentInfoMap.get(segment);
      if (segmentInfo == null) {
        // NOTE: This should not happen, but we still handle it gracefully by adding an invalid SegmentInfo
        LOGGER.error("Failed to find segment info for segment: {} in table: {} while handling assignment change",
            segment, _tableNameWithType);
        segmentInfo =
            new SegmentInfo(INVALID_PARTITION_ID, INVALID_CREATION_TIME_MS, getOnlineServers(externalView, segment));
        _segmentInfoMap.put(segment, segmentInfo);
      } else {
        segmentInfo._onlineServers = getOnlineServers(externalView, segment);
      }
    }
    _segmentInfoMap.keySet().retainAll(onlineSegments);
    computeAllTablePartitionInfo();
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    int partitionId = getPartitionId(segment, znRecord);
    long pushTimeMs = getCreationTimeMs(znRecord);
    SegmentInfo segmentInfo = _segmentInfoMap.get(segment);
    if (segmentInfo == null) {
      // NOTE: This should not happen, but we still handle it gracefully by adding an invalid SegmentInfo
      LOGGER.error("Failed to find segment info for segment: {} in table: {} while handling segment refresh", segment,
          _tableNameWithType);
      segmentInfo = new SegmentInfo(partitionId, pushTimeMs, Collections.emptyList());
      _segmentInfoMap.put(segment, segmentInfo);
    } else {
      segmentInfo._partitionId = partitionId;
      segmentInfo._creationTimeMs = pushTimeMs;
    }
    computeAllTablePartitionInfo();
  }

  public TablePartitionInfo getTablePartitionInfo() {
    return _tablePartitionInfo;
  }

  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo() {
    return _tablePartitionReplicatedServersInfo;
  }

  private void computeAllTablePartitionInfo() {
    computeTablePartitionReplicatedServersInfo();
    computeTablePartitionInfo();
  }

  private void computeTablePartitionReplicatedServersInfo() {
    PartitionInfo[] partitionInfoMap = new PartitionInfo[_numPartitions];
    List<String> segmentsWithInvalidPartition = new ArrayList<>();
    List<Pair<String, Integer>> unavailableSegments = new ArrayList<>();
    List<Triple<String, Integer, Integer>> segmentsReducingFullyReplicatedServers = new ArrayList<>();
    List<Map.Entry<String, SegmentInfo>> newSegmentInfoEntries = new ArrayList<>();
    long currentTimeMs = System.currentTimeMillis();
    for (Map.Entry<String, SegmentInfo> entry : _segmentInfoMap.entrySet()) {
      String segment = entry.getKey();
      SegmentInfo segmentInfo = entry.getValue();
      int partitionId = segmentInfo._partitionId;
      if (partitionId == INVALID_PARTITION_ID) {
        segmentsWithInvalidPartition.add(segment);
        continue;
      }
      // Process new segments in the end
      if (InstanceSelector.isNewSegment(segmentInfo._creationTimeMs, currentTimeMs)) {
        newSegmentInfoEntries.add(entry);
        continue;
      }
      List<String> onlineServers = segmentInfo._onlineServers;
      PartitionInfo partitionInfo = partitionInfoMap[partitionId];
      if (onlineServers.isEmpty()) {
        unavailableSegments.add(Pair.of(segment, partitionId));
        if (partitionInfo == null) {
          List<String> segments = new ArrayList<>();
          segments.add(segment);
          partitionInfo = new PartitionInfo(new HashSet<>(), segments);
          partitionInfoMap[partitionId] = partitionInfo;
        } else {
          partitionInfo._fullyReplicatedServers.clear();
          partitionInfo._segments.add(segment);
        }
      } else {
        if (partitionInfo == null) {
          Set<String> fullyReplicatedServers = new HashSet<>(onlineServers);
          List<String> segments = new ArrayList<>();
          segments.add(segment);
          partitionInfo = new PartitionInfo(fullyReplicatedServers, segments);
          partitionInfoMap[partitionId] = partitionInfo;
        } else {
          if (partitionInfo._fullyReplicatedServers.retainAll(onlineServers)) {
            segmentsReducingFullyReplicatedServers.add(
                Triple.of(segment, partitionId, partitionInfo._fullyReplicatedServers.size()));
          }
          partitionInfo._segments.add(segment);
        }
      }
    }
    if (!segmentsWithInvalidPartition.isEmpty()) {
      int numSegments = segmentsWithInvalidPartition.size();
      LOGGER.warn("(table-partition-rs-info) Found {} segments: {} with invalid partition in table: {}", numSegments,
          numSegments <= 10 ? segmentsWithInvalidPartition : segmentsWithInvalidPartition.subList(0, 10) + "...",
          _tableNameWithType);
    }
    if (!unavailableSegments.isEmpty()) {
      int numSegments = unavailableSegments.size();
      LOGGER.warn("Found {} unavailable segments (name,partition): {} in table: {}", numSegments,
          numSegments <= 10 ? unavailableSegments : unavailableSegments.subList(0, 10) + "...", _tableNameWithType);
    }
    if (!segmentsReducingFullyReplicatedServers.isEmpty()) {
      int numSegments = segmentsReducingFullyReplicatedServers.size();
      LOGGER.warn("Found {} segments (name,partition,reducedTo): {} reducing fully replicated servers in table: {}",
          numSegments, numSegments <= 10 ? segmentsReducingFullyReplicatedServers
              : segmentsReducingFullyReplicatedServers.subList(0, 10) + "...", _tableNameWithType);
    }
    // Process new segments
    if (!newSegmentInfoEntries.isEmpty()) {
      List<String> excludedNewSegments = new ArrayList<>();
      for (Map.Entry<String, SegmentInfo> entry : newSegmentInfoEntries) {
        String segment = entry.getKey();
        SegmentInfo segmentInfo = entry.getValue();
        int partitionId = segmentInfo._partitionId;
        List<String> onlineServers = segmentInfo._onlineServers;
        PartitionInfo partitionInfo = partitionInfoMap[partitionId];
        if (partitionInfo == null) {
          // If the new segment is the first segment of a partition, treat it as regular segment if it has available
          // replicas
          if (!onlineServers.isEmpty()) {
            Set<String> fullyReplicatedServers = new HashSet<>(onlineServers);
            List<String> segments = new ArrayList<>();
            segments.add(segment);
            partitionInfo = new PartitionInfo(fullyReplicatedServers, segments);
            partitionInfoMap[partitionId] = partitionInfo;
          } else {
            excludedNewSegments.add(segment);
          }
        } else {
          // If the new segment is not the first segment of a partition, add it only if it won't reduce the fully
          // replicated servers. It is common that a new created segment (newly pushed, or a new consuming segment)
          // doesn't have all the replicas available yet, and we want to exclude it from the partition info until all
          // the replicas are available.
          //noinspection SlowListContainsAll
          if (onlineServers.containsAll(partitionInfo._fullyReplicatedServers)) {
            partitionInfo._segments.add(segment);
          } else {
            excludedNewSegments.add(segment);
          }
        }
      }
      if (!excludedNewSegments.isEmpty()) {
        int numSegments = excludedNewSegments.size();
        LOGGER.info("Excluded {} new segments: {}... without all replicas available in table: {}", numSegments,
            numSegments <= 10 ? excludedNewSegments : excludedNewSegments.subList(0, 10) + "...", _tableNameWithType);
      }
    }
    _tablePartitionReplicatedServersInfo =
        new TablePartitionReplicatedServersInfo(_tableNameWithType, _partitionColumn, _partitionFunctionName,
            _numPartitions, partitionInfoMap, segmentsWithInvalidPartition);
  }

  private void computeTablePartitionInfo() {
    List<List<String>> segmentsByPartition = new ArrayList<>();
    for (int i = 0; i < _numPartitions; i++) {
      segmentsByPartition.add(new ArrayList<>());
    }
    List<String> segmentsWithInvalidPartition = new ArrayList<>();
    for (Map.Entry<String, SegmentInfo> entry : _segmentInfoMap.entrySet()) {
      String segment = entry.getKey();
      SegmentInfo segmentInfo = entry.getValue();
      int partitionId = segmentInfo._partitionId;
      if (partitionId == INVALID_PARTITION_ID) {
        segmentsWithInvalidPartition.add(segment);
      } else if (partitionId < segmentsByPartition.size()) {
        segmentsByPartition.get(partitionId).add(segment);
      } else {
        LOGGER.warn("Found segment: {} with partitionId: {} larger than numPartitions: {} in table: {}",
            segment, partitionId, _numPartitions, _tableNameWithType);
        segmentsWithInvalidPartition.add(segment);
      }
    }
    if (!segmentsWithInvalidPartition.isEmpty()) {
      int numSegments = segmentsWithInvalidPartition.size();
      LOGGER.warn("(table-partition-info) Found {} segments: {} with invalid partition in table: {}", numSegments,
          numSegments <= 10 ? segmentsWithInvalidPartition : segmentsWithInvalidPartition.subList(0, 10) + "...",
          _tableNameWithType);
    }
    _tablePartitionInfo =
        new TablePartitionInfo(_tableNameWithType, _partitionColumn, _partitionFunctionName, _numPartitions,
            segmentsByPartition, segmentsWithInvalidPartition);
  }

  private static class SegmentInfo {
    int _partitionId;
    long _creationTimeMs;
    List<String> _onlineServers;

    SegmentInfo(int partitionId, long creationTimeMs, List<String> onlineServers) {
      _partitionId = partitionId;
      _creationTimeMs = creationTimeMs;
      _onlineServers = onlineServers;
    }
  }
}

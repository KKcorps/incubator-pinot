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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class UpsertTableSegmentPreloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME_1 = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME_2 = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME + "_2");


  // Assume that we have segment data for the second table similar to the first table
  private static final String UPLOADED_SEGMENT_1_TABLE_1 = "mytable_10027_19736_0 %";
  private static final String UPLOADED_SEGMENT_2_TABLE_1 = "mytable_10072_19919_1 %";
  private static final String UPLOADED_SEGMENT_3_TABLE_1 = "mytable_10158_19938_2 %";

  private static final String UPLOADED_SEGMENT_1_TABLE_2 = "mytable_2_10027_19736_0 %";
  private static final String UPLOADED_SEGMENT_2_TABLE_2 = "mytable_2_10072_19919_1 %";
  private static final String UPLOADED_SEGMENT_3_TABLE_2 = "mytable_2_10158_19938_2 %";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka and push data into Kafka
    startKafka();
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(avroFiles);

    populateTable(REALTIME_TABLE_NAME_1, avroFiles);
    populateTable(REALTIME_TABLE_NAME_2, avroFiles);
  }

  protected void populateTable(String tableName, List<File> avroFiles)
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);


    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig =
        createUpsertTableConfig(tableName, avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    tableConfig.getUpsertConfig().setEnablePreload(true);
    tableConfig.getUpsertConfig().setEnableSnapshot(true);
    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(tableName, TableType.REALTIME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(tableName, 600_000L);
  }

  protected TableConfig createUpsertTableConfig(String tableName, File sampleAvroFile, String primaryKeyColumn,
      String deleteColumn, int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(deleteColumn);

    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setLLC(useLlc()).setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setUpsertConfig(upsertConfig).build();
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads",
        "1");
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, System.getProperty("user.dir") + "/MULTI_TABLE");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropTable(REALTIME_TABLE_NAME_1);
    dropTable(REALTIME_TABLE_NAME_2);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void dropTable(String tableName)
      throws IOException {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    // Test dropping all segments one by one
    List<String> segments = listSegments(realtimeTableName);
    assertFalse(segments.isEmpty());
    for (String segment : segments) {
      dropSegment(realtimeTableName, segment);
    }
    // NOTE: There is a delay to remove the segment from property store
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return listSegments(realtimeTableName).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");

    dropRealtimeTable(realtimeTableName);
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
  }

  @Override
  protected String getSchemaName() {
    return "upsertSchema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_upload_segment_test.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 3;
  }

  protected void waitForAllDocsLoaded(String tableName, long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert(tableName) == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, 5000L, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  private long getCurrentCountStarResultWithoutUpsert(String tableName) {
    long result = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
    return result;
  }

  private long getCountStarResultWithoutUpsert() {
    // 3 Avro files, each with 100 documents, one copy from streaming source, one copy from batch source
    return 600;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    verifyIdealState(REALTIME_TABLE_NAME_1, 5);
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState(REALTIME_TABLE_NAME_1, 5);
    assertEquals(getCurrentCountStarResult(REALTIME_TABLE_NAME_1), getCountStarResult());
    assertEquals(getCurrentCountStarResultWithoutUpsert(REALTIME_TABLE_NAME_1), getCountStarResultWithoutUpsert());
    waitForSnapshotCreation(REALTIME_TABLE_NAME_1);

    verifyIdealState(REALTIME_TABLE_NAME_2, 5);
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState(REALTIME_TABLE_NAME_2, 5);
    assertEquals(getCurrentCountStarResult(REALTIME_TABLE_NAME_2), getCountStarResult());
    assertEquals(getCurrentCountStarResultWithoutUpsert(REALTIME_TABLE_NAME_2), getCountStarResultWithoutUpsert());
    waitForSnapshotCreation(REALTIME_TABLE_NAME_2);

    restartServers();

    verifyIdealState(REALTIME_TABLE_NAME_1, 7);
    waitForAllDocsLoaded(REALTIME_TABLE_NAME_1, 600_000L);

    verifyIdealState(REALTIME_TABLE_NAME_2, 7);
    waitForAllDocsLoaded(REALTIME_TABLE_NAME_2, 600_000L);
  }

  protected void waitForSnapshotCreation(String tableName)
      throws Exception {
    Set<String> consumingSegments = getConsumingSegmentsFromIdealState(tableName);
    // trigger force commit for snapshots
    String jobId = forceCommit(tableName);

    Set<String> finalConsumingSegments = consumingSegments;

    TestUtils.waitForCondition(aVoid -> {
      try {
        if (isForceCommitJobCompleted(jobId)) {
          assertTrue(_controllerStarter.getHelixResourceManager()
              .getOnlineSegmentsFromIdealState(tableName, false)
              .containsAll(finalConsumingSegments));

          int snapshotFileCount = 0;
          for (BaseServerStarter serverStarter : _serverStarters) {
            String segmentDir =
                serverStarter.getConfig().getProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR);
            File[] files = new File(segmentDir, tableName).listFiles();
            for (File file : files) {
              if (!file.getName().startsWith(getTableName())) {
                continue;
              }
              if (file.isDirectory()) {
                File segmentV3Dir = new File(file, "v3");
                File[] segmentFiles = segmentV3Dir.listFiles();
                for (File segmentFile : segmentFiles) {
                  if (segmentFile.getName().endsWith(".snapshot")) {
                    snapshotFileCount++;
                  }
                }
              }
            }
          }
          return snapshotFileCount == 5;
        }
        return false;
      } catch (Exception e) {
        return false;
      }
    }, 120000L, "Error verifying force commit operation on table!");
  }

  protected void verifyIdealState(String tableName, int numSegmentsExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, tableName);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    String serverForPartition0 = null;
    String serverForPartition1 = null;

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, SegmentStateModel.ONLINE);
        } else {
          assertEquals(state, SegmentStateModel.CONSUMING);
        }
      } else {
        assertEquals(state, SegmentStateModel.ONLINE);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      if (partitionId == 0) {
        if (serverForPartition0 == null) {
          serverForPartition0 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition0);
        }
      } else {
        assertEquals(partitionId, 1);
        if (serverForPartition1 == null) {
          serverForPartition1 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition1);
        }
      }
    }
  }

  protected Set<String> getConsumingSegmentsFromIdealState(String tableNameWithType) {
    IdealState tableIdealState = _controllerStarter.getHelixResourceManager().getTableIdealState(tableNameWithType);
    Map<String, Map<String, String>> segmentAssignment = tableIdealState.getRecord().getMapFields();
    Set<String> matchingSegments = new HashSet<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        matchingSegments.add(entry.getKey());
      }
    }
    return matchingSegments;
  }

  protected boolean isForceCommitJobCompleted(String forceCommitJobId)
      throws Exception {
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forForceCommitJobStatus(forceCommitJobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    assertEquals(jobStatus.get("jobId").asText(), forceCommitJobId);
    assertEquals(jobStatus.get("jobType").asText(), "FORCE_COMMIT");
    return jobStatus.get("numberOfSegmentsYetToBeCommitted").asInt(-1) == 0;
  }

  protected String forceCommit(String tableName)
      throws Exception {
    String response = sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName), null);
    return JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
  }

  private static int getSegmentPartitionId(String segmentName) {
    switch (segmentName) {
      case UPLOADED_SEGMENT_1_TABLE_1:
      case UPLOADED_SEGMENT_1_TABLE_2:
        return 0;
      case UPLOADED_SEGMENT_2_TABLE_1:
      case UPLOADED_SEGMENT_3_TABLE_1:
      case UPLOADED_SEGMENT_2_TABLE_2:
      case UPLOADED_SEGMENT_3_TABLE_2:
        return 1;
      default:
        return new LLCSegmentName(segmentName).getPartitionGroupId();
    }
  }
}

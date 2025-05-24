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
package org.apache.pinot.controller.validation;

import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.validation.RealtimeConsumptionManager;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RealtimeConsumptionManagerTest {
  @Mock
  private PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;

  @Mock
  private ResourceUtilizationManager _resourceUtilizationManager;

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @Mock
  private StorageQuotaChecker _storageQuotaChecker;

  @Mock
  private ControllerMetrics _controllerMetrics;

  private AutoCloseable _mocks;
  private RealtimeConsumptionManager _realtimeConsumptionManager;

  @BeforeMethod
  public void setup() {
    ControllerConf controllerConf = new ControllerConf();
    _mocks = MockitoAnnotations.openMocks(this);
    _realtimeConsumptionManager =
        new RealtimeConsumptionManager(controllerConf, _pinotHelixResourceManager, null, _llcRealtimeSegmentManager,
            _controllerMetrics, _storageQuotaChecker, _resourceUtilizationManager);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testEnforcePauseState() {
    String tableName = "testTable_REALTIME";
    PauseStatusDetails pauseStatus = mock(PauseStatusDetails.class);
    TableConfig tableConfig = mock(TableConfig.class);

    when(pauseStatus.getPauseFlag()).thenReturn(false);
    when(pauseStatus.getReasonCode()).thenReturn(PauseState.ReasonCode.NONE);
    when(_llcRealtimeSegmentManager.getPauseStatusDetails(tableName)).thenReturn(pauseStatus);
    when(_resourceUtilizationManager.isResourceUtilizationWithinLimits(tableName)).thenReturn(true);
    when(_pinotHelixResourceManager.getTableConfig(tableName)).thenReturn(tableConfig);
    when(_storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig)).thenReturn(false);

    _realtimeConsumptionManager.enforcePauseState(tableName, tableConfig);

    verify(_llcRealtimeSegmentManager, never()).pauseConsumption(anyString(), any(), anyString());
  }
}

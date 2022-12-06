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
package org.apache.pinot.segment.local.data.manager;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;


/**
 * The config used for TableDataManager.
 */
public class TableDataManagerConfig {
  public static final String AUTH_CONFIG_PREFIX = "auth";

  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final TableConfig _tableConfig;

  public TableDataManagerConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    _tableConfig = tableConfig;
  }

  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public String getTableName() {
    return _tableConfig.getTableName();
  }

  public TableType getTableType() {
    return _tableConfig.getTableType();
  }

  public boolean isDimTable() {
    return _tableConfig.isDimTable();
  }

  public String getDataDir() {
    return _instanceDataManagerConfig.getInstanceDataDir() + "/" + getTableName();
  }

  public String getConsumerDir() {
    return _instanceDataManagerConfig.getConsumerDir();
  }

  public String getTablePeerDownloadScheme() {
    String peerSegmentDownloadScheme = _tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
    if (peerSegmentDownloadScheme != null) {
      return peerSegmentDownloadScheme;
    }
    return _instanceDataManagerConfig.getSegmentPeerDownloadScheme();
  }

  public int getTableDeletedSegmentsCacheSize() {
    return _instanceDataManagerConfig.getDeletedSegmentsCacheSize();
  }

  public int getTableDeletedSegmentsCacheTtlMinutes() {
    return _instanceDataManagerConfig.getDeletedSegmentsCacheTtlMinutes();
  }

  public Configuration getAuthConfig() {
    Configuration authConfig = new PropertiesConfiguration();
    _instanceDataManagerConfig.getConfig().subset(AUTH_CONFIG_PREFIX).toMap().forEach(authConfig::addProperty);
    return authConfig;
  }
}

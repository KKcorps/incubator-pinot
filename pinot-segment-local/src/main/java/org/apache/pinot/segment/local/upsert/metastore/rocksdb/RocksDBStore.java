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
package org.apache.pinot.segment.local.upsert.metastore.rocksdb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pinot.segment.local.upsert.metastore.KVStore;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDBStore implements KVStore<ColumnFamilyHandle, byte[], byte[]> {
  private WriteOptions _writeOptions;
  private ReadOptions _readOptions;

  private RocksDB _rocksDB;
  private String _storagePath;

  Logger _logger = LoggerFactory.getLogger(getClass().getSimpleName());

  @Override
  public void init(Map<String, String> config) {
    try {
      _storagePath = RocksDBUtils.getRocksDBStoragePath(config);
      Options options = RocksDBUtils.getRocksDBOptions(config);
      _rocksDB = RocksDB.open(options, _storagePath);
      _readOptions = new ReadOptions();
      _writeOptions = new WriteOptions();
      _logger.info("Created RocksDB instance for path: {}", _storagePath);
    } catch (Exception e) {
      _logger.error("Cannot open RocksDB database on path: {}", _storagePath, e);
    }
  }

  @Override
  public ColumnFamilyHandle addShard(String shardName)
      throws Exception {
    ColumnFamilyDescriptor columnFamilyDescriptor =
        new ColumnFamilyDescriptor((shardName).getBytes(StandardCharsets.UTF_8));
    ColumnFamilyHandle columnFamilyHandle = _rocksDB.createColumnFamily(columnFamilyDescriptor);
    return columnFamilyHandle;
  }

  @Override
  public byte[] get(ColumnFamilyHandle cfHandle, byte[] key)
      throws Exception {
    return _rocksDB.get(cfHandle, _readOptions, key);
  }

  @Override
  public void put(ColumnFamilyHandle cfHandle, byte[] key, byte[] value)
      throws Exception {
    _rocksDB.put(cfHandle, _writeOptions, key, value);
  }

  @Override
  public void delete(ColumnFamilyHandle cfHandle, byte[] key)
      throws Exception {
    _rocksDB.delete(cfHandle, _writeOptions, key);
  }

  @Override
  public long getNumKeys(ColumnFamilyHandle cfHandle) {
    try {
      return _rocksDB.getLongProperty(cfHandle, "rocksdb.estimate-num-keys");
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public void close()
      throws IOException {
    _logger.info("Closing RocksDB instance for path: {}", _storagePath);
    _rocksDB.close();
  }
}

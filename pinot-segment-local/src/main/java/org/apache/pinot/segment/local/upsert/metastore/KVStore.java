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
package org.apache.pinot.segment.local.upsert.metastore;

import java.io.Closeable;
import java.util.Map;


public interface KVStore<SK, TK, TV> extends Closeable {

  void init(Map<String, String> config);

  TV get(SK partition, TK key) throws Exception;

  void put(SK partition, TK key, TK value) throws Exception;

  void delete(SK partition, TK key) throws Exception;

  long getNumKeys(SK shard);

  SK addShard(String shardName) throws Exception;
}

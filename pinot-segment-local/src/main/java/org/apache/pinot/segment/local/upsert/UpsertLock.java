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
package org.apache.pinot.segment.local.upsert;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class UpsertLock {

  private UpsertLock() {
  }

  private static final int NUM_LOCKS = 10000;
  private static final Lock[] LOCKS = new Lock[NUM_LOCKS];

  static {
    for (int i = 0; i < NUM_LOCKS; i++) {
      LOCKS[i] = new ReentrantLock();
    }
  }

  public static Lock getPrimaryKeyLock(PrimaryKey primaryKey) {
    return LOCKS[Math.abs(primaryKey.hashCode() % NUM_LOCKS)];
  }
}

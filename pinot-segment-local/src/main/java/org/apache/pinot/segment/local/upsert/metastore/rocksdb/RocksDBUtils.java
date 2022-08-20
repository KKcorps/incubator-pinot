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

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import java.io.File;
import java.util.Map;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;


public class RocksDBUtils {
  public static final String ROCKSDB_BLOOM_FILTER_BITS = "rocksdb.bloom_filter.bits";
  public static final String ROCKSDB_BLOCK_CACHE_SIZE = "rocksdb.block_cache.size.bytes";
  public static final String ROCKSDB_ROW_CACHE_SIZE = "rocksdb.row_cache.size.bytes";
  public static final String ROCKSDB_STATS_DUMP_PERIOD_SECONDS = "rocksdb.stats_dump.period.seconds";
  public static final String ROCKSDB_LOG_LEVEL = "rocksdb.log.level";
  public static final String ROCKSDB_CACHE_FILTERS_ENABLED = "rocksdb.cache_filters.enabled";
  public static final String ROCKSDB_HASH_INDEX_ENABLED = "rocksdb.hash_index.enabled";
  public static final String ROCKSDB_PREFIX_EXTRACTOR_LENGTH = "rocksdb.prefix_extractor.length";
  public static final String ROCKSDB_MAX_OPEN_FILES = "rocksdb.max_open_files";
  public static final String ROCKSDB_MAX_BACKGROUND_THREADS = "rocksdb.max_background_threads";
  public static final String DATA_DIR = "data.dir";

  public static final String DEFAULT_ROCKSDB_BLOOM_FILTER_BITS = "10";
  public static final String DEFAULT_ROCKSDB_BLOCK_CACHE_SIZE_BYTES = String.valueOf(1024L * 1024 * 1024);
  public static final String DEFAULT_ROCKSDB_ROW_CACHE_SIZE_BYTES = String.valueOf(1024L * 1024 * 1024);
  public static final String DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SECONDS = "300";
  public static final String DEFAULT_ROCKSDB_LOG_LEVEL = InfoLogLevel.INFO_LEVEL.toString();
  public static final String DEFAULT_ROCKSDB_CACHE_FILTERS_ENABLED = "true";
  public static final String DEFAULT_ROCKSDB_HASH_INDEX_ENABLED = "true";
  public static final String DEFAULT_ROCKSDB_PREFIX_EXTRACTOR_LENGTH = "10";
  public static final String DEFAULT_ROCKSDB_MAX_OPEN_FILES = "64";
  public static final String DEFAULT_ROCKSDB_MAX_BACKGROUND_THREADS = "4";
  public static final String ROCKSDB_DIR = "rocksDB";

  private RocksDBUtils() {
  }

  public static String getRocksDBStoragePath(Map<String, String> config) {
    String dataDir = config.get(DATA_DIR);
    if (dataDir == null) {
      File tmpDir = Files.createTempDir();
      dataDir = tmpDir.getAbsolutePath();
    }

    String storageDir = Joiner.on("/").join(dataDir, ROCKSDB_DIR);

    return storageDir;
  }

  public static Options getRocksDBOptions(Map<String, String> config) {
    Options dbOptions = new Options();
    dbOptions.setCreateIfMissing(true);

    BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(5); // using latest file format

    boolean enableHashIndex =
        Boolean.parseBoolean(config.getOrDefault(ROCKSDB_HASH_INDEX_ENABLED, DEFAULT_ROCKSDB_HASH_INDEX_ENABLED));

    if (enableHashIndex) {
      blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
      blockBasedTableConfig.setDataBlockHashTableUtilRatio(0.75);

      blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
      dbOptions.useCappedPrefixExtractor(Integer.parseInt(
          config.getOrDefault(ROCKSDB_PREFIX_EXTRACTOR_LENGTH, DEFAULT_ROCKSDB_PREFIX_EXTRACTOR_LENGTH)));
    }

    blockBasedTableConfig.setFilterPolicy(new BloomFilter(
        Integer.parseInt(config.getOrDefault(ROCKSDB_BLOOM_FILTER_BITS, DEFAULT_ROCKSDB_BLOOM_FILTER_BITS))));
    blockBasedTableConfig.setBlockCache(new LRUCache(
        Long.parseLong(config.getOrDefault(ROCKSDB_BLOCK_CACHE_SIZE, DEFAULT_ROCKSDB_BLOCK_CACHE_SIZE_BYTES))));

    // Pin and Cache blocks
    boolean enableFilterCaching =
        Boolean.parseBoolean(config.getOrDefault(ROCKSDB_CACHE_FILTERS_ENABLED, DEFAULT_ROCKSDB_CACHE_FILTERS_ENABLED));

    if (enableFilterCaching) {
      blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
      blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
      blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
      blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
      blockBasedTableConfig.setOptimizeFiltersForMemory(true);
    }

    dbOptions.setTableFormatConfig(blockBasedTableConfig);

    dbOptions.setMaxOpenFiles(Integer.parseInt(config.getOrDefault(ROCKSDB_MAX_OPEN_FILES,
        DEFAULT_ROCKSDB_MAX_OPEN_FILES))); // allow multiple sstable files to be opened, good when you have to do
      // random lreads
    dbOptions.setInfoLogLevel(
        InfoLogLevel.valueOf(config.getOrDefault(ROCKSDB_LOG_LEVEL, DEFAULT_ROCKSDB_LOG_LEVEL))); // disable logging
    dbOptions.setStatsDumpPeriodSec(Integer.parseInt(config.getOrDefault(ROCKSDB_STATS_DUMP_PERIOD_SECONDS,
        DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SECONDS))); // disable dumping rocksDB stats in logs
    dbOptions.setMemtableWholeKeyFiltering(true);
    dbOptions.setMemtablePrefixBloomSizeRatio(0.1);

    //Add row cache and use faster compression (default: snappy)
    dbOptions.setRowCache(new LRUCache(
        Long.parseLong(config.getOrDefault(ROCKSDB_ROW_CACHE_SIZE, DEFAULT_ROCKSDB_ROW_CACHE_SIZE_BYTES))));
    dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);

    dbOptions.setMaxBackgroundJobs(
        Integer.parseInt(config.getOrDefault(ROCKSDB_MAX_BACKGROUND_THREADS, DEFAULT_ROCKSDB_MAX_BACKGROUND_THREADS)));

    return dbOptions;
  }
}

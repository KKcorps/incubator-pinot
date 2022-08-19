package org.apache.pinot.segment.local.upsert.metastore.rocksdb;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.segment.local.upsert.metastore.KVStore;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDBStore implements KVStore<byte[], byte[]> {
  private WriteOptions _writeOptions;
  private ReadOptions _readOptions;

  private RocksDB _rocksDB;

  Logger _logger = LoggerFactory.getLogger(getClass().getSimpleName());

  @Override
  public void init(Map<String, String> config) {
    String storagePath = "";
    try {
      storagePath = RocksDBUtils.getRocksDBStoragePath(config);
      Options options = RocksDBUtils.getRocksDBOptions(config);
      _rocksDB = RocksDB.open(options, RocksDBUtils.getRocksDBStoragePath(config));
      _readOptions = new ReadOptions();
      _writeOptions = new WriteOptions();
    } catch (Exception e) {
      _logger.error("Cannot open RocksDB database on path: {}", storagePath, e);
    }
  }

  @Override
  public byte[] get(byte[] key) throws Exception {
      return _rocksDB.get(_readOptions, key);
  }

  @Override
  public void put(byte[] key, byte[] value) throws Exception  {
    _rocksDB.put(_writeOptions, key, value);
  }

  @Override
  public void delete(byte[] key) throws Exception  {
    _rocksDB.delete(_writeOptions, key);
  }

  @Override
  public long getNumKeys() {
    try {
      return Long.parseLong(_rocksDB.getProperty("rocksdb.estimate-num-keys"));
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public void close()
      throws IOException {
    _rocksDB.close();
  }


}

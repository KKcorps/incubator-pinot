package org.apache.pinot.segment.local.upsert.metastore;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import org.rocksdb.RocksDBException;


public interface KVStore<TK, TV> extends Closeable {

  void init(Map<String, String> config);

  TV get(TK key) throws Exception;

  void put(TK key, TK value) throws Exception;

  void delete(TK key) throws Exception;

  long getNumKeys();
}

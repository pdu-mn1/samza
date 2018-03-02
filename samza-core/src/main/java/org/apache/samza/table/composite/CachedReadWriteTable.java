package org.apache.samza.table.composite;

import java.util.List;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;


public class CachedReadWriteTable<K, V> extends CachedReadableTable<K, V> implements ReadWriteTable<K, V> {
  private ReadWriteTable<K, V> innerTable;

  public CachedReadWriteTable(ReadWriteTable<K, V> innerTable, Cache<K, V> cache) {
    super(innerTable, cache);
    this.innerTable = innerTable;
  }

  @Override
  public void put(K key, V value) {
    innerTable.put(key, value);
    cache.put(key, value);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    entries.forEach(e -> put(e.getKey(), e.getValue()));
  }

  @Override
  public void delete(K key) {
    innerTable.delete(key);
    cache.invalidate(key);
  }

  @Override
  public void deleteAll(List<K> keys) {
    keys.forEach(k -> delete(k));
  }

  @Override
  public void flush() {
    innerTable.flush();
  }
}

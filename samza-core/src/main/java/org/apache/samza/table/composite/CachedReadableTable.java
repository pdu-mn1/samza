package org.apache.samza.table.composite;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.table.ReadableTable;


public class CachedReadableTable<K, V> implements ReadableTable<K, V> {
  private final ReadableTable<K, V> innerTable;
  protected final Cache<K, V> cache;

  public CachedReadableTable(ReadableTable<K, V> innerTable, Cache<K, V> cache) {
    this.innerTable = innerTable;
    this.cache = cache;
  }

  @Override
  public V get(K key) {
    V retVal = cache.get(key);
    if (retVal == null) {
      retVal = innerTable.get(key);
      cache.put(key, retVal);
    }
    return retVal;
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    return keys.stream().collect(Collectors.toMap(Function.identity(), k -> get(k)));
  }

  @Override
  public void close() {
  }
}

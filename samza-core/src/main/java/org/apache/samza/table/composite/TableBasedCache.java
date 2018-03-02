package org.apache.samza.table.composite;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.task.TaskContext;


public class TableBasedCache<K, V> implements Cache<K, V> {
  private final String name;
  private ReadWriteTable<K, V> table;
  private final AtomicInteger hitCount = new AtomicInteger(1);
  private final AtomicInteger totalAccess = new AtomicInteger(1); // avoid div-by-0
  private CacheGauge<Double> hitRate;

  public TableBasedCache(String name, ReadWriteTable<K, V> table) {
    this.table = table;
    this.name = name;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    hitRate = new CacheGauge("hitRate", () -> hitCount.get() / totalAccess.get());
    taskContext.getMetricsRegistry().newGauge(name, hitRate);
  }

  @Override
  public V get(K key) {
    V value = table.get(key);
    if (value != null) {
      hitCount.incrementAndGet();
    }
    totalAccess.incrementAndGet();
    return value;
  }

  @Override
  public void put(K key, V value) {
    table.put(key, value);
  }

  @Override
  public void invalidate(K key) {
    table.delete(key);
  }
}

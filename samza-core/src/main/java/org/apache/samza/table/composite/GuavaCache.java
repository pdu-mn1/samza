package org.apache.samza.table.composite;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.task.TaskContext;

import com.google.common.cache.CacheStats;


public class GuavaCache<K, V> implements Cache<K, V> {
  private final com.google.common.cache.Cache<K, V> cache;
  private final String name;

  public GuavaCache(String name, com.google.common.cache.Cache<K, V> cache) {
    this.name = name;
    this.cache = cache;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    CacheStats stats = cache.stats();
    Gauge<Double> hitRate = new CacheGauge("hitRate", () -> stats.hitRate());
    taskContext.getMetricsRegistry().newGauge(name, hitRate);
    Counter evictCount = new CacheCounter("evictcount", () -> stats.evictionCount());
    taskContext.getMetricsRegistry().newCounter(name, evictCount);
  }

  @Override
  public V get(K key) {
    return cache.getIfPresent(key);
  }

  @Override
  public void put(K key, V value) {
    cache.put(key, value);
  }

  @Override
  public void invalidate(K key) {
    cache.invalidate(key);
  }
}

/*
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

package org.apache.samza.table.caching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.utils.CallbackUtils;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.TableOpCallback;
import org.apache.samza.table.utils.DefaultTableReadMetrics;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;


/**
 * A composite table incorporating a cache with a Samza table. The cache is
 * represented as a {@link ReadWriteTable}.
 *
 * The intented use case is to optimize the latency of accessing the actual table, eg.
 * remote tables, when eventual consistency between cache and table is acceptable.
 * The cache is expected to support TTL such that the values can be refreshed at some
 * point.
 *
 * If the actual table is read-write table, CachingTable supports both write-through
 * and write-around (writes bypassing cache) policies. For write-through policy, it
 * supports read-after-write semantics because the value is cached after written to
 * the table.
 *
 * Table and cache are updated (put/delete) in an atomic manner as such it is thread
 * safe for concurrent accesses. Strip locks are used for fine-grained synchronization
 * and the number of stripes is configurable.
 *
 * NOTE: Cache get is not synchronized with put for better parallelism in the read path.
 * As such, cache table implementation is expected to be thread-safe for concurrent
 * accesses.
 *
 * @param <K> type of the table key
 * @param <V> type of the table value
 */
public class CachingTable<K, V> implements ReadWriteTable<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(CachingTable.class);

  private final String tableId;
  private final ReadableTable<K, V> rdTable;
  private final ReadWriteTable<K, V> rwTable;
  private final ReadWriteTable<K, V> cache;
  private final boolean isWriteAround;

  // Use stripe based locking to allow parallelism of disjoint keys.
  private final Striped<Lock> stripedLocks;

  // Metrics
  private DefaultTableReadMetrics readMetrics;
  private DefaultTableWriteMetrics writeMetrics;

  private CallbackUtils callbackHelper;

  // Common caching stats
  private AtomicLong hitCount = new AtomicLong();
  private AtomicLong missCount = new AtomicLong();

  public CachingTable(String tableId, ReadableTable<K, V> table, ReadWriteTable<K, V> cache, int stripes, boolean isWriteAround) {
    this.tableId = tableId;
    this.rdTable = table;
    this.rwTable = table instanceof ReadWriteTable ? (ReadWriteTable) table : null;
    this.cache = cache;
    this.isWriteAround = isWriteAround;
    this.stripedLocks = Striped.lazyWeakLock(stripes);
    this.callbackHelper = new CallbackUtils(tableId, LOG);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    readMetrics = new DefaultTableReadMetrics(containerContext, taskContext, this, tableId);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    tableMetricsUtil.newGauge("hit-rate", () -> hitRate());
    tableMetricsUtil.newGauge("miss-rate", () -> missRate());
    tableMetricsUtil.newGauge("req-count", () -> requestCount());
  }

  @Override
  public V get(K key) {
    readMetrics.numGets.inc();
    long startNs = System.nanoTime();
    V value = cache.get(key);
    if (value == null) {
      Lock lock = stripedLocks.get(key);
      try {
        lock.lock();
        if (cache.get(key) == null) {
          missCount.incrementAndGet();
          // Due to the lack of contains() API in ReadableTable, there is
          // no way to tell whether a null return by cache.get(key) means
          // cache miss or the value is actually null. As such, we cannot
          // support negative cache semantics.
          value = rdTable.get(key);
          if (value != null) {
            cache.put(key, value);
          }
        }
      } finally {
        lock.unlock();
      }
    } else {
      hitCount.incrementAndGet();
    }
    readMetrics.getNs.update(System.nanoTime() - startNs);
    return value;
  }

  @Override
  public void get(K key, TableOpCallback<V> callback) {
    readMetrics.numGets.inc();
    long startNs = System.nanoTime();
    // Blocking get on cache
    V value = cache.get(key);
    if (value == null) {
      missCount.incrementAndGet();

      // No locking here because we cannot block in async context.
      // As such it is possible to create duplicate async requests
      // with concurrent get of the same key until cache is updated.
      rdTable.get(key, (newVal, error) -> {
          if (error == null) {
            Lock lock = stripedLocks.get(key);
            try {
              lock.lock();
              if (cache.get(key) == null) {
                cache.put(key, newVal);
              }
            } finally {
              lock.unlock();
            }
          }
          readMetrics.getNs.update(System.nanoTime() - startNs);
          callbackHelper.invoke(callback, newVal, error, readMetrics.getCallbackNs);
        });
    } else {
      hitCount.incrementAndGet();
      readMetrics.getNs.update(System.nanoTime() - startNs);
      // Notify immediately if cache is hit
      callbackHelper.invoke(callback, value, null, readMetrics.getCallbackNs);
    }
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    readMetrics.numGetAlls.inc();
    long startNs = System.nanoTime();
    Map<K, V> getAllResult = new HashMap<>();
    // Blocking get on cache
    List<K> missingKeys = lookup(keys, getAllResult);

    if (!missingKeys.isEmpty()) {
      // Batch fetch all missing keys.
      Iterable<Lock> locks = stripedLocks.bulkGet(keys);
      Map<K, V> values = Collections.EMPTY_MAP;
      try {
        locks.forEach(Lock::lock);
        values = rdTable.getAll(missingKeys);
        // Update cache with fetched values
        values.forEach((k, v) -> cache.put(k, v));
      } finally {
        locks.forEach(Lock::unlock);
      }

      // Update result with fetched entries
      getAllResult.putAll(values);
    }

    readMetrics.getAllNs.update(System.nanoTime() - startNs);

    return getAllResult;
  }

  @Override
  public void getAll(List<K> keys, TableOpCallback<Map<K, V>> callback) {
    readMetrics.numGetAlls.inc();
    long startNs = System.nanoTime();
    Map<K, V> getAllResult = new HashMap<>();
    // Blocking get on cache
    List<K> missingKeys = lookup(keys, getAllResult);

    hitCount.addAndGet(keys.size() - missingKeys.size());
    missCount.addAndGet(missingKeys.size());

    if (!missingKeys.isEmpty()) {
      // Batch fetch all missing keys.
      // No locking here because we cannot block in async context.
      // As such it is possible to create duplicate async requests
      // with concurrent get of the same keys until cache is updated.
      rdTable.getAll(missingKeys, (values, error) -> {
          if (error == null) {
            // Update result with fetched values
            getAllResult.putAll(values);

            // Update cache with fetched values
            values.forEach((k, v) -> cache.put(k, v));
          }
          readMetrics.getAllNs.update(System.nanoTime() - startNs);
          callbackHelper.invoke(callback, getAllResult, error, readMetrics.getCallbackNs);
        });
    } else {
      // Notify immediately if all keys are hit
      callback.onComplete(getAllResult, null);
    }
  }

  @Override
  public void put(K key, V value) {
    writeMetrics.numPuts.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    Lock lock = stripedLocks.get(key);
    try {
      lock.lock();
      rwTable.put(key, value);
      if (!isWriteAround) {
        cache.put(key, value);
      }
    } finally {
      lock.unlock();
    }
    writeMetrics.putNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void put(K key, V value, TableOpCallback callback) {
    writeMetrics.numPuts.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);

    rwTable.put(key, value, (dummy, error) -> {
        if (error != null && !isWriteAround) {
          Lock lock = stripedLocks.get(key);
          try {
            lock.lock();
            cache.put(key, value);
          } finally {
            lock.unlock();
          }
        }
        writeMetrics.putNs.update(System.nanoTime() - startNs);
        callbackHelper.invoke(callback, dummy, error, writeMetrics.putCallbackNs);
      });
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    writeMetrics.numPutAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    Set<K> keys = entries.stream().map(Entry::getKey).collect(Collectors.toSet());
    Iterable<Lock> locks = stripedLocks.bulkGet(keys);
    try {
      locks.forEach(Lock::lock);
      rwTable.putAll(entries);
      if (!isWriteAround) {
        cache.putAll(entries);
      }
    } finally {
      locks.forEach(Lock::unlock);
    }
    writeMetrics.putAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, TableOpCallback callback) {
    writeMetrics.numPutAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot write to a read-only table: " + rdTable);
    rwTable.putAll(entries, (dummy, error) -> {
        if (error != null && !isWriteAround) {
          Set<K> keys = entries.stream().map(Entry::getKey).collect(Collectors.toSet());
          Iterable<Lock> locks = stripedLocks.bulkGet(keys);
          try {
            locks.forEach(Lock::lock);
            cache.putAll(entries);
          } finally {
            locks.forEach(Lock::unlock);
          }
        }
        writeMetrics.putAllNs.update(System.nanoTime() - startNs);
        callbackHelper.invoke(callback, dummy, error, writeMetrics.putCallbackNs);
      });
  }

  @Override
  public void delete(K key) {
    writeMetrics.numDeletes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    Lock lock = stripedLocks.get(key);
    try {
      lock.lock();
      rwTable.delete(key);
      cache.delete(key);
    } finally {
      lock.unlock();
    }
    writeMetrics.deleteNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void delete(K key, TableOpCallback callback) {
    writeMetrics.numDeletes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    rwTable.delete(key, (dummy, error) -> {
        if (error != null) {
          Lock lock = stripedLocks.get(key);
          try {
            lock.lock();
            cache.delete(key);
          } finally {
            lock.unlock();
          }
        }
        writeMetrics.deleteNs.update(System.nanoTime() - startNs);
        callbackHelper.invoke(callback, dummy, error, writeMetrics.deleteCallbackNs);
      });
  }

  @Override
  public void deleteAll(List<K> keys) {
    writeMetrics.numDeleteAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    Iterable<Lock> locks = stripedLocks.bulkGet(keys);
    try {
      locks.forEach(Lock::lock);
      rwTable.deleteAll(keys);
      cache.deleteAll(keys);
    } finally {
      locks.forEach(Lock::unlock);
    }
    writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void deleteAll(List<K> keys, TableOpCallback callback) {
    writeMetrics.numDeleteAlls.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot delete from a read-only table: " + rdTable);
    rwTable.deleteAll(keys, (dummy, error) -> {
        if (error != null) {
          Iterable<Lock> locks = stripedLocks.bulkGet(keys);
          try {
            locks.forEach(Lock::lock);
            cache.deleteAll(keys);
          } finally {
            locks.forEach(Lock::unlock);
          }
        }
        writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
        callbackHelper.invoke(callback, dummy, error, writeMetrics.deleteCallbackNs);
      });
  }

  @Override
  public synchronized void flush() {
    writeMetrics.numFlushes.inc();
    long startNs = System.nanoTime();
    Preconditions.checkNotNull(rwTable, "Cannot flush a read-only table: " + rdTable);
    rwTable.flush();
    writeMetrics.flushNs.update(System.nanoTime() - startNs);
  }

  @Override
  public void close() {
    this.cache.close();
    this.rdTable.close();
  }

  private List<K> lookup(List<K> keys, Map<K, V> entries) {
    List<K> missKeys = new ArrayList<>();
    keys.forEach(k -> {
        V value = cache.get(k);
        if (value == null) {
          missKeys.add(k);
        } else {
          entries.put(k, value);
        }
      });
    return missKeys;
  }

  double hitRate() {
    long reqs = requestCount();
    return reqs == 0 ? 1.0 : (double) hitCount.get() / reqs;
  }

  double missRate() {
    long reqs = requestCount();
    return reqs == 0 ? 1.0 : (double) missCount.get() / reqs;
  }

  long requestCount() {
    return hitCount.get() + missCount.get();
  }
}

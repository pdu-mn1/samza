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

package org.apache.samza.table.remote;

import java.util.List;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.TableOpCallback;
import org.apache.samza.table.utils.DefaultTableWriteMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Remote store backed read writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadWriteTable<K, V> extends RemoteReadableTable<K, V> implements ReadWriteTable<K, V> {
  private final TableWriteFunction<K, V> writeFn;

  private DefaultTableWriteMetrics writeMetrics;

  @VisibleForTesting
  Throttler writeThrottler;

  public RemoteReadWriteTable(String tableId, TableReadFunction readFn, TableWriteFunction writeFn,
      Throttler<K, V> readThrottler, Throttler<K, V> writeThrottler, RequestManager<K, V> requestManager) {
    super(tableId, readFn, readThrottler, requestManager);
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    this.writeThrottler = writeThrottler;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    super.init(containerContext, taskContext);
    writeMetrics = new DefaultTableWriteMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    writeThrottler.setTimerMetric(tableMetricsUtil.newTimer("put-throttleKeys-ns"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) {
    Preconditions.checkNotNull(key);
    if (value == null) {
      delete(key);
      return;
    }

    try {
      writeMetrics.numPuts.inc();
      long startNs = System.nanoTime();
      requestManager.execute(key, value, () -> writeFn.put(key, value), writeThrottler);
      writeMetrics.putNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put a record, key=%s, value=%s", key, value);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value, TableOpCallback callback) {
    Preconditions.checkNotNull(key);
    if (value == null) {
      delete(key, callback);
      return;
    }

    writeMetrics.numPuts.inc();

    requestManager.executeAsync(key, value, () -> {
        final long startNs = System.nanoTime();
        writeFn.put(key, value, (dummy, error) -> {
            writeMetrics.putNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error(String.format("Failed to put a record, key=%s, value=%s", key, value), error);
            }
            callbackHelper.invoke(callback, dummy, error, writeMetrics.putCallbackNs);
          });
      }, writeThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(List<Entry<K, V>> entries) {
    Preconditions.checkNotNull(entries);
    if (entries.isEmpty()) {
      return;
    }

    try {
      writeMetrics.numPutAlls.inc();
      long startNs = System.nanoTime();
      requestManager.executeEntries(entries, () -> writeFn.putAll(entries), writeThrottler);
      writeMetrics.putAllNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put records: %s", entries);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries, TableOpCallback callback) {
    Preconditions.checkNotNull(entries);
    if (entries.isEmpty()) {
      callbackHelper.invoke(callback, null, null, writeMetrics.putCallbackNs);
      return;
    }

    writeMetrics.numPutAlls.inc();
    final long startNs = System.nanoTime();

    requestManager.executeEntriesAsync(entries, () ->
        writeFn.putAll(entries, (dummy, error) -> {
            writeMetrics.putAllNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error(String.format("Failed to put records: %s", entries), error);
            }
            callbackHelper.invoke(callback, dummy, error, writeMetrics.putCallbackNs);
          }), writeThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(K key) {
    Preconditions.checkNotNull(key);
    try {
      writeMetrics.numDeletes.inc();
      long startNs = System.nanoTime();
      requestManager.execute(key, null, () -> writeFn.delete(key), writeThrottler); // FIXME
      writeMetrics.deleteNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void delete(K key, TableOpCallback callback) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(callback);
    writeMetrics.numDeletes.inc();
    final long startNs = System.nanoTime();
    requestManager.executeAsync(key, () -> {
        writeFn.delete(key, (dummy, error) -> {
            writeMetrics.deleteNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error(String.format("Failed to delete a record, key=%s", key), error);
            }
            callbackHelper.invoke(callback, dummy, error, writeMetrics.deleteCallbackNs);
          });
      }, writeThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteAll(List<K> keys) {
    Preconditions.checkNotNull(keys);
    if (keys.isEmpty()) {
      return;
    }
    try {
      writeMetrics.numDeleteAlls.inc();
      long startNs = System.nanoTime();
      requestManager.execute(keys, () -> { writeFn.deleteAll(keys); return null; }, writeThrottler); // FIXME
      writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete records, keys=%s", keys);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }
  @Override
  public void deleteAll(List<K> keys, TableOpCallback callback) {
    Preconditions.checkNotNull(keys);
    Preconditions.checkNotNull(callback);
    if (keys.isEmpty()) {
      callbackHelper.invoke(callback, null, null, writeMetrics.deleteCallbackNs);
      return;
    }
    writeMetrics.numDeleteAlls.inc();
    final long startNs = System.nanoTime();

    requestManager.executeAsync(keys, () -> {
        writeFn.deleteAll(keys, (dummy, error) -> {
            writeMetrics.deleteAllNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error(String.format("Failed to delete records, keys=%s", keys), error);
            }
            callbackHelper.invoke(callback, dummy, error, writeMetrics.deleteCallbackNs);
          });
      }, writeThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    try {
      writeMetrics.numFlushes.inc();
      long startNs = System.nanoTime();
      writeFn.flush();
      writeMetrics.flushNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    writeFn.close();
    super.close();
  }
}

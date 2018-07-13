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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.TableOpCallback;
import org.apache.samza.table.utils.CallbackUtils;
import org.apache.samza.table.utils.DefaultTableReadMetrics;
import org.apache.samza.table.utils.TableMetricsUtil;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * A Samza {@link org.apache.samza.table.Table} backed by a remote data-store or service.
 * <p>
 * Many stream-processing applications require to look-up data from remote data sources eg: databases,
 * web-services, RPC systems to process messages in the stream. Such access to adjunct datasets can be
 * naturally modeled as a join between the incoming stream and a {@link RemoteReadableTable}.
 * <p>
 * Example use-cases include:
 * <ul>
 *  <li> Augmenting a stream of "page-views" with information from a database of user-profiles; </li>
 *  <li> Scoring page views with impressions services. </li>
 *  <li> A notifications-system that sends out emails may require a query to an external database to process its message. </li>
 * </ul>
 * <p>
 * A {@link RemoteReadableTable} is meant to be used with a {@link TableReadFunction} and a {@link TableWriteFunction}
 * which encapsulate the functionality of reading and writing data to the remote service. These provide a
 * pluggable means to specify I/O operations on the table. While the base implementation merely delegates to
 * these reader and writer functions, sub-classes of {@link RemoteReadableTable} may provide rich functionality like
 * caching or throttling on top of them.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadableTable<K, V> implements ReadableTable<K, V> {

  protected final String tableId;
  protected final Logger logger;
  protected final CallbackUtils callbackHelper;

  @VisibleForTesting
  final Throttler<K, V> readThrottler;
  final RequestManager<K, V> requestManager;

  private final TableReadFunction<K, V> readFn;
  private DefaultTableReadMetrics readMetrics;

  /**
   * Construct a RemoteReadableTable instance
   * @param tableId table id
   * @param readFn {@link TableReadFunction} for read operations
   * @param throttler throttler for rate limiting
   * @param requestManager request manager
   */
  public RemoteReadableTable(String tableId, TableReadFunction<K, V> readFn,
      Throttler<K, V> throttler, RequestManager<K, V> requestManager) {
    Preconditions.checkArgument(tableId != null && !tableId.isEmpty(), "invalid table id");
    Preconditions.checkNotNull(readFn, "null read function");
    this.tableId = tableId;
    this.readFn = readFn;
    this.logger = LoggerFactory.getLogger(getClass().getName() + "-" + tableId);
    this.callbackHelper = new CallbackUtils(tableId, logger);
    this.readThrottler = throttler;
    this.requestManager = requestManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    readMetrics = new DefaultTableReadMetrics(containerContext, taskContext, this, tableId);
    TableMetricsUtil tableMetricsUtil = new TableMetricsUtil(containerContext, taskContext, this, tableId);
    readThrottler.setTimerMetric(tableMetricsUtil.newTimer("get-throttle-ns"));
    requestManager.setRetryCounter(tableMetricsUtil.newCounter("retry-count"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(K key) {
    Preconditions.checkNotNull(key);

    try {
      readMetrics.numGets.inc();
      long startNs = System.nanoTime();
      V result = requestManager.execute(key, () -> readFn.get(key), readThrottler);
      readMetrics.getNs.update(System.nanoTime() - startNs);
      return result;
    } catch (Exception e) {
      String errMsg = String.format("Failed to get a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void get(K key, TableOpCallback<V> callback) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(callback);
    readMetrics.numGets.inc();
    requestManager.executeAsync(key, () -> {
        final long startNs = System.nanoTime();
        readFn.get(key, requestManager.decorate((value, error) -> {
            readMetrics.getNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error(String.format("Failed to get a record, key=%s", key), error);
            }
            callbackHelper.invoke(callback, value, error, readMetrics.getCallbackNs);
          }));
      }, readThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(List<K> keys) {
    Preconditions.checkNotNull(keys);
    if (keys.isEmpty()) {
      return Collections.EMPTY_MAP;
    }

    final Map<K, V> result;
    try {
      readMetrics.numGetAlls.inc();
      long startNs = System.nanoTime();
      result = requestManager.execute(keys, () -> readFn.getAll(keys), readThrottler);
      readMetrics.getAllNs.update(System.nanoTime() - startNs);
    } catch (Exception e) {
      String errMsg = "Failed to get some records";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }

    if (result == null) {
      String errMsg = String.format("Received null records, keys=%s", keys);
      logger.error(errMsg);
      throw new SamzaException(errMsg);
    }

    if (result.size() < keys.size()) {
      String errMsg = String.format("Received insufficient number of records (%d), keys=%s", result.size(), keys);
      logger.error(errMsg);
      throw new SamzaException(errMsg);
    }

    return result;
  }

  @Override
  public void getAll(List<K> keys, TableOpCallback<Map<K, V>> callback) {
    Preconditions.checkNotNull(keys);
    Preconditions.checkNotNull(callback);
    if (keys.isEmpty()) {
      callbackHelper.invoke(callback, Collections.EMPTY_LIST, null, readMetrics.getCallbackNs);
    }

    readMetrics.numGetAlls.inc();

    final long startNs = System.nanoTime();
    requestManager.executeAsync(keys, () -> {
        readFn.getAll(keys, requestManager.decorate((values, error) -> {
            readMetrics.getAllNs.update(System.nanoTime() - startNs);
            if (error != null) {
              logger.error("Failed to get some records", error);
            } else if (values == null) {
              String errMsg = String.format("Received null records, keys=%s", keys);
              logger.error(errMsg);
              error = new SamzaException(errMsg);
            } else if (values.size() < keys.size()) {
              String errMsg = String.format("Received insufficient number of records (%d), keys=%s", values.size(), keys);
              logger.error(errMsg);
              error = new SamzaException(errMsg);
            }
            callbackHelper.invoke(callback, values, error, readMetrics.getCallbackNs);
          }));
      }, readThrottler);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    readFn.close();
  }
}

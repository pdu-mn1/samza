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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.apache.samza.SamzaException;
import org.apache.samza.storage.kv.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/**
 * Helper class to handle table IO requests with rate limiting and retries on behalf of the remote table.
 *
 * It provides two sets of APIs: blocking and non-blocking, ie. execute{Entries} and execute{Entries}Async.
 * Blocking requests are executed immediately whereas non-blocking requests are buffered in a blocking queue.
 * If the blocking queue is filled up, further non-blocking requests will be blocked to apply back pressure.
 *
 * In either mode, multiple versions of execute() are provided for the possible CRUD operations, ie.
 * (key), (key, value), (keys), (entries).
 *
 * Both read and write operations should share the same RequestManager instance for each remote table.
 *
 * @param <K> type of the table key
 * @param <V> type of the table record
 */
public class RequestManager<K, V> implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RequestManager.class);

  private final String tableId;

  @VisibleForTesting
  final BlockingQueue<Request> requestQueue;

  private volatile boolean shutdown = false;

  /**
   * Wrapper of a queued request
   */
  static class Request {
    final int credits;
    final Runnable runnable;
    final Throttler throttler;

    long retryCount;
    long previousBackoff;

    public Request(int credits, Runnable runnable, Throttler throttler) {
      this.credits = credits;
      this.runnable = runnable;
      this.throttler = throttler;
      this.retryCount = 0;
      this.previousBackoff = 0;
    }
  }

  public RequestManager(String tableId, int maxRequests) {
    this.requestQueue = new ArrayBlockingQueue<>(maxRequests);
    this.tableId = tableId;

    Executors.newSingleThreadExecutor((arg) -> {
        Thread thread = new Thread(arg);
        thread.setName(tableId + "-request-manager");
        thread.setDaemon(true);
        return thread;
      }).submit(this);
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        Request request = requestQueue.take();
        request.throttler.throttle(request.credits);
        request.runnable.run();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for async request, shutdown=" + shutdown);
        if (shutdown) {
          break;
        }
      } catch (Exception e) {
        LOG.error("Failed to submit async request for table={}", tableId);
        // TODO:
      }
    }
    LOG.info("RequestManager has exited, table={}", tableId);
  }

  /**
   * Throttle the request given a table record (key, value)
   * @param key key of the table record
   * @param method method to be executed
   * @param throttler throttler instance
   * @param <R> return type
   */
  public <R> R execute(K key, Supplier<R> method, Throttler<K, V> throttler) {
    return doExecute(throttler.getCredits(key, null), method, throttler);
  }

  /**
   * Throttle the request given a table record (key, value)
   * @param key key of the table record
   * @param value value of the table record
   * @param method method to be executed
   * @param throttler throttler instance
   */
  public void execute(K key, V value, Runnable method, Throttler<K, V> throttler) {
    doExecute(throttler.getCredits(key, value), () -> { method.run(); return null; }, throttler);
  }

  /**
   * Throttle the request given a list of table keys
   * @param keys list of keys
   * @param method method to be executed
   * @param throttler throttler instance
   * @param <R> return type
   */
  public <R> R execute(List<K> keys, Supplier<R> method, Throttler<K, V> throttler) {
    return doExecute(throttler.getCredits(keys), method, throttler);
  }

  /**
   * Throttle the request given a list of table records
   * @param entries list of records
   * @param method method to be executed
   * @param throttler throttler instance
   */
  // Have to be renamed because of type erasure
  public void executeEntries(List<Entry<K, V>> entries, Runnable method, Throttler<K, V> throttler) {
    doExecute(throttler.getEntryCredits(entries), () -> { method.run(); return (Void) null; }, throttler);
  }

  private <R> R doExecute(int credits, Supplier<R> method, Throttler<K, V> throttler) {
    try {
      throttler.throttle(credits);
      return method.get();
    } catch (Exception e) {
      LOG.error("Failed to execute table operation after all retries, table={}.", tableId);
      throw new SamzaException("Table operation failed.", e);
    }
  }

  /**
   * Asynchronously throttle the request given a table record
   * @param key key of the table record
   * @param method method to be executed
   * @param throttler throttler instance
   */
  public void executeAsync(K key, Runnable method, Throttler<K, V> throttler) {
    executeAsync(key, null, method, throttler);
  }

  /**
   * Asynchronously throttle the request given a table record
   * @param key key of the table record
   * @param value value of the table record
   * @param method method to be executed
   * @param throttler throttler instance
   */
  public void executeAsync(K key, V value, Runnable method, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getCredits(key, value), method, throttler));
  }

  /**
   * Asynchronously throttle the request given a list of table keys
   * @param keys list of keys
   * @param method method to be executed
   * @param throttler throttler instance
   */
  public void executeAsync(List<K> keys, Runnable method, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getCredits(keys), method, throttler));
  }

  /**
   * Asynchronously throttle the request given a list of table records
   * @param entries list of records
   * @param method method to be executed
   * @param throttler throttler instance
   */
  // Have to be renamed because of type erasure
  public void executeEntriesAsync(List<Entry<K, V>> entries, Runnable method, Throttler<K, V> throttler) {
    requestQueue.add(new Request(throttler.getEntryCredits(entries), method, throttler));
  }

  public void shutdown() {
    this.shutdown = true;
    this.requestQueue.clear();
  }
}

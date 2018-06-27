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

import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Helper class for remote table to throttle table IO requests with the configured rate limiter.
 * It provides two sets of APIs: blocking and non-blocking, ie. throttle{Entries} and throttle{Entries}Async.
 * For each request, the needed credits are calculated with the configured credit function which is passed
 * to rate limiter. Throttler queues all async (request, credit) pairs in a blocking queue with configurable
 * capacity. If the capacity is exhausted, further requests will be blocked to apply back pressure.
 *
 * For either mode, four versions of the throttle methods are provided for the possible CRUD operations, ie.
 * (key), (key, value), (keys), (entries).
 *
 * @param <K> type of the table key
 * @param <V> type of the table record
 */
public class Throttler<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(Throttler.class);

  private final String tag;
  private final boolean rateLimited;
  private final CreditFunction<K, V> creditFn;

  @VisibleForTesting
  final RateLimiter rateLimiter;

  private Timer waitTimeMetric;

  public Throttler(String tableId, RateLimiter rateLimiter, CreditFunction<K, V> creditFn, String tag) {
    this.rateLimiter = rateLimiter;
    this.creditFn = creditFn;
    this.tag = tag;
    this.rateLimited = rateLimiter != null && rateLimiter.getSupportedTags().contains(tag);
    LOG.info("Rate limiting is {} for {}", rateLimited ? "enabled" : "disabled", tableId);
  }

  /**
   * Set up waitTimeMetric metric for latency reporting due to throttling.
   * @param timer waitTimeMetric metric
   */
  public void setTimerMetric(Timer timer) {
    Preconditions.checkNotNull(timer);
    this.waitTimeMetric = timer;
  }

  public int getCredits(K key, V value) {
    return (creditFn == null) ? 1 : creditFn.getCredits(key, value);
  }

  public int getCredits(List<K> keys) {
    if (creditFn == null) {
      return keys.size();
    } else {
      return keys.stream().mapToInt(k -> creditFn.getCredits(k, null)).sum();
    }
  }

  public int getEntryCredits(List<Entry<K, V>> entries) {
    if (creditFn == null) {
      return entries.size();
    } else {
      return entries.stream().mapToInt(e -> creditFn.getCredits(e.getKey(), e.getValue())).sum();
    }
  }

  public void throttle(int credits) {
    if (!rateLimited) {
      return;
    }

    long startNs = System.nanoTime();
    rateLimiter.acquire(Collections.singletonMap(tag, credits));
    waitTimeMetric.update(System.nanoTime() - startNs);
  }
}

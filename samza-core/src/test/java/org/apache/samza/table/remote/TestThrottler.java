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

import java.util.Arrays;
import java.util.Collections;

import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.util.RateLimiter;
import org.junit.Test;

import junit.framework.Assert;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestThrottler {
  private static final String DEFAULT_TAG = "mytag";

  public Throttler<String, String> getThrottler() {
    return getThrottler(DEFAULT_TAG);
  }

  public Throttler<String, String> getThrottler(String tag) {
    CreditFunction<String, String> credFn = (CreditFunction<String, String>) (key, value) -> {
      int credits = key == null ? 0 : 1;
      credits += value == null ? 0 : 1;
      return credits;
    };
    RateLimiter rateLimiter = mock(RateLimiter.class);
    doReturn(Collections.singleton(DEFAULT_TAG)).when(rateLimiter).getSupportedTags();
    Throttler<String, String> throttler = new Throttler<>("foo", rateLimiter, credFn, tag);
    Timer timer = mock(Timer.class);
    throttler.setTimerMetric(timer);
    return throttler;
  }

  @Test
  public void testCreditKeyOnly() {
    Throttler<String, String> throttler = getThrottler();
    Assert.assertEquals(1, throttler.getCredits("abc", null));
  }

  @Test
  public void testCreditKeyValue() {
    Throttler<String, String> throttler = getThrottler();
    Assert.assertEquals(2, throttler.getCredits("abc", "efg"));
  }

  @Test
  public void testCreditKeys() {
    Throttler<String, String> throttler = getThrottler();
    Assert.assertEquals(3, throttler.getCredits(Arrays.asList("abc", "efg", "hij")));
  }

  @Test
  public void testCreditEntries() {
    Throttler<String, String> throttler = getThrottler();
    Assert.assertEquals(4, throttler.getEntryCredits(
        Arrays.asList(new Entry<>("abc", "efg"), new Entry<>("hij", "lmn"))));
  }

  @Test
  public void testThrottle() {
    Throttler<String, String> throttler = getThrottler();
    Timer timer = mock(Timer.class);
    throttler.setTimerMetric(timer);
    throttler.throttle(1000);
    verify(throttler.rateLimiter, times(1)).acquire(anyMap());
    verify(timer, times(1)).update(anyLong());
  }

  @Test
  public void testThrottleUnknownTag() {
    Throttler<String, String> throttler = getThrottler("unknown_tag");
    throttler.throttle(1000);
    verify(throttler.rateLimiter, times(0)).acquire(anyMap());
  }
}

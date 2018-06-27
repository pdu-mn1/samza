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
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.EmbeddedTaggedRateLimiter;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.table.remote.RemoteTableDescriptor.RL_READ_TAG;
import static org.apache.samza.table.remote.RemoteTableDescriptor.RL_WRITE_TAG;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestRemoteTableDescriptor {
  private void doTestSerialize(RateLimiter rateLimiter,
      CreditFunction readCredFn,
      CreditFunction writeCredFn) {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    desc.withWriteFunction(mock(TableWriteFunction.class));
    if (rateLimiter != null) {
      desc.withRateLimiter(rateLimiter, readCredFn, writeCredFn);
    } else {
      desc.withReadRateLimit(100);
      desc.withWriteRateLimit(200);
    }
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTableProvider.RATE_LIMITER));
    Assert.assertEquals(readCredFn != null, spec.getConfig().containsKey(RemoteTableProvider.READ_CREDIT_FN));
    Assert.assertEquals(writeCredFn != null, spec.getConfig().containsKey(RemoteTableProvider.WRITE_CREDIT_FN));
  }

  @Test
  public void testSerializeSimple() {
    doTestSerialize(null, null, null);
  }

  @Test
  public void testSerializeWithLimiter() {
    doTestSerialize(mock(RateLimiter.class), null, null);
  }

  @Test
  public void testSerializeWithLimiterAndReadCredFn() {
    doTestSerialize(mock(RateLimiter.class), (k, v) -> 1, null);
  }

  @Test
  public void testSerializeWithLimiterAndWriteCredFn() {
    doTestSerialize(mock(RateLimiter.class), null, (k, v) -> 1);
  }

  @Test
  public void testSerializeWithLimiterAndReadWriteCredFns() {
    doTestSerialize(mock(RateLimiter.class), (key, value) -> 1, (key, value) -> 1);
  }

  @Test
  public void testSerializeNullWriteFunction() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTableProvider.READ_FN));
    Assert.assertFalse(spec.getConfig().containsKey(RemoteTableProvider.WRITE_FN));
  }

  @Test(expected = NullPointerException.class)
  public void testSerializeNullReadFunction() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTableProvider.READ_FN));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpecifyBothRateAndRateLimiter() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    desc.withReadRateLimit(100);
    desc.withRateLimiter(mock(RateLimiter.class), null, null);
    desc.getTableSpec();
  }

  private TaskContext createMockTaskContext() {
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    TaskContext taskContext = mock(TaskContext.class);
    doReturn(metricsRegistry).when(taskContext).getMetricsRegistry();
    SamzaContainerContext containerCtx = new SamzaContainerContext(
        "1", null, Collections.singleton(new TaskName("MyTask")), null);
    doReturn(containerCtx).when(taskContext).getSamzaContainerContext();
    return taskContext;
  }

  static class CountingCreditFunction<K, V> implements CreditFunction<K, V> {
    int numCalls = 0;
    @Override
    public int getCredits(K key, V value) {
      numCalls++;
      return 1;
    }
  }

  private void doTestDeserializeReadFunctionAndLimiter(boolean rateOnly, boolean rlGets, boolean rlPuts) {
    int numRateLimitOps = (rlGets ? 1 : 0) + (rlPuts ? 1 : 0);
    RemoteTableDescriptor<String, String> desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    desc.withWriteFunction(mock(TableWriteFunction.class));
    desc.withMaxAsyncRequests(1000);
    desc.withMaxRetryCount(123);
    desc.withRetryBackoffMultiplier(5.0);
    desc.withInitialRetryBackoffMs(300);
    desc.withMaxRetryBackoffMs(50000);

    if (rateOnly) {
      if (rlGets) {
        desc.withReadRateLimit(1000);
      }
      if (rlPuts) {
        desc.withWriteRateLimit(2000);
      }
    } else {
      if (numRateLimitOps > 0) {
        Map<String, Integer> tagCredits = new HashMap<>();
        if (rlGets) {
          tagCredits.put(RL_READ_TAG, 1000);
        }
        if (rlPuts) {
          tagCredits.put(RL_WRITE_TAG, 2000);
        }

        // Spy the rate limiter to verify call count
        RateLimiter rateLimiter = spy(new EmbeddedTaggedRateLimiter(tagCredits));
        desc.withRateLimiter(rateLimiter, new CountingCreditFunction(), new CountingCreditFunction());
      }
    }

    TableSpec spec = desc.getTableSpec();
    RemoteTableProvider provider = new RemoteTableProvider(spec);
    provider.init(mock(SamzaContainerContext.class), createMockTaskContext());
    Table table = provider.getTable();
    Assert.assertTrue(table instanceof RemoteReadWriteTable);
    RemoteReadWriteTable rwTable = (RemoteReadWriteTable) table;
    if (numRateLimitOps > 0) {
      Assert.assertTrue(!rlGets || rwTable.readThrottler != null);
      Assert.assertTrue(!rlPuts || rwTable.writeThrottler != null);
    }

    Assert.assertNotNull(rwTable.requestManager.retryHelper);
    Assert.assertEquals(rwTable.requestManager.retryHelper.getMaxRetryCount(), 123);
    Assert.assertTrue(rwTable.requestManager.requestQueue.remainingCapacity() == 1000);

    // Verify rate limiter usage
    if (numRateLimitOps > 0) {
      rwTable.get("xxx");
      rwTable.put("yyy", "zzz");

      if (!rateOnly) {
        verify(rwTable.readThrottler.rateLimiter, times(numRateLimitOps)).acquire(any());
      } else {
        Assert.assertEquals(rwTable.readThrottler.rateLimiter.getSupportedTags().size(), numRateLimitOps);

        if (rlGets) {
          Assert.assertTrue(rwTable.readThrottler.rateLimiter.getSupportedTags().contains(RL_READ_TAG));
        }
        if (rlPuts) {
          Assert.assertTrue(rwTable.writeThrottler.rateLimiter.getSupportedTags().contains(RL_WRITE_TAG));
        }
      }
    }
  }

  @Test
  public void testDeserializeReadFunctionNoRateLimit() {
    doTestDeserializeReadFunctionAndLimiter(false, false, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterWrite() {
    doTestDeserializeReadFunctionAndLimiter(false, false, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRead() {
    doTestDeserializeReadFunctionAndLimiter(false, true, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterReadWrite() {
    doTestDeserializeReadFunctionAndLimiter(false, true, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyWrite() {
    doTestDeserializeReadFunctionAndLimiter(true, false, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyRead() {
    doTestDeserializeReadFunctionAndLimiter(true, true, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyReadWrite() {
    doTestDeserializeReadFunctionAndLimiter(true, true, true);
  }
}

package org.apache.samza.table.composite;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.table.composite.CachedTableProvider.CACHE;
import static org.apache.samza.table.composite.CachedTableProvider.INNER_TABLE_ID;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestCachedTable {
  static class MockReadableTable implements ReadableTable {
    protected final Map store = new HashMap<>();

    @Override
    public Object get(Object key) {
      return store.get(key);
    }

    @Override
    public Map getAll(List keys) {
      return (Map) keys.stream().collect(Collectors.toMap(Function.identity(), k -> get(k)));
    }

    @Override
    public void close() {

    }
  }

  static class MockReadWriteTable extends MockReadableTable implements ReadWriteTable {
    @Override
    public void put(Object key, Object value) {
      store.put(key, value);
    }

    @Override
    public void putAll(List list) {
      list.stream().forEach(e -> put(((Entry<?, ?>) e).getKey(), ((Entry<?, ?>) e).getValue()));
    }

    @Override
    public void delete(Object key) {
      store.remove(key);
    }

    @Override
    public void deleteAll(List keys) {
      keys.forEach(k -> store.remove(k));
    }

    @Override
    public void flush() {

    }
  }

  private TaskContext createMockTaskContext(MockReadableTable table) {
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Gauge.class)).when(metricsRegistry).newGauge(anyString(), anyObject());
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), (Counter) anyObject());
    TaskContext taskContext = mock(TaskContext.class);
    doReturn(metricsRegistry).when(taskContext).getMetricsRegistry();
    SamzaContainerContext containerCtx = new SamzaContainerContext(
        "1", null, Collections.singleton(new TaskName("MyTask")), null);
    doReturn(containerCtx).when(taskContext).getSamzaContainerContext();
    doReturn(table).when(taskContext).getTable("myInnerTable");
    return taskContext;
  }


  @Test
  public void testCachedTable() {
    Cache<String, String> cache = mock(Cache.class);
    Map<String, String> configs = new HashMap<>();
    configs.put(CACHE, CachedTableDescriptor.serializeObject("cache", cache));
    configs.put(INNER_TABLE_ID, "myInnerTable");
    TableSpec spec = new TableSpec("myTable", null, "", configs);

    CachedTableProvider provider = new CachedTableProvider(spec);
    TaskContext context = createMockTaskContext(new MockReadWriteTable());
    provider.init(mock(SamzaContainerContext.class), context);

    CachedReadWriteTable rwTable = (CachedReadWriteTable) provider.getTable();
    Assert.assertNull(rwTable.get("123"));
    rwTable.put("123", "457");
    Assert.assertNotNull(rwTable.get("123"));

    verify(rwTable.cache, times(2)).get(any());

    // mock cache does not remember anything
    verify(rwTable.cache, times(3)).put(any(), any());
  }
}

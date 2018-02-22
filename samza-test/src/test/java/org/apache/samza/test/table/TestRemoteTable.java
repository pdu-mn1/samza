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

package org.apache.samza.test.table;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.RemoteReadWriteTable;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;


public class TestRemoteTable extends AbstractIntegrationTestHarness {
  private TableReadFunction<Integer, TestTableData.Profile> getInMemoryReader(TestTableData.Profile[] profiles) {
    final Map<Integer, TestTableData.Profile> profileMap = Arrays.stream(profiles)
        .collect(Collectors.toMap(p -> p.getMemberId(), Function.identity()));
    TableReadFunction<Integer, TestTableData.Profile> reader =
        (TableReadFunction<Integer, TestTableData.Profile>) key -> profileMap.getOrDefault(key, null);
    return reader;
  }

  static List<TestTableData.EnrichedPageView> writtenRecords = new LinkedList<>();

  static class InMemoryRecordWriter implements TableWriteFunction<Integer, TestTableData.EnrichedPageView> {
    private transient List<TestTableData.EnrichedPageView> records;

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      records = writtenRecords;
    }

    @Override
    public void put(Integer key, TestTableData.EnrichedPageView record) {
      records.add(record);
    }

    @Override
    public void delete(Integer key) {
      records.remove(key);
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
      records.removeAll(keys);
    }
  }

  @Test
  public void testStreamTableJoinRemoteTable() throws Exception {
    List<TestTableData.PageView> received = new LinkedList<>();
    final InMemoryRecordWriter writer = new InMemoryRecordWriter();

    int count = 10;
    TestTableData.PageView[] pageViews = TestTableData.generatePageViews(count);
    TestTableData.Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = TestLocalTable.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {
      RemoteTableDescriptor<Integer, TestTableData.Profile> inputTableDesc = new RemoteTableDescriptor<>("profile-table-1");
      inputTableDesc.withReadFunction(getInMemoryReader(profiles));

      RemoteTableDescriptor<Integer, TestTableData.EnrichedPageView> outputTableDesc = new RemoteTableDescriptor<>("enriched-page-view-table-1");
      outputTableDesc
          .withReadFunction(key -> null) // dummy reader
          .withWriteFunction(writer);

      Table<KV<Integer, TestTableData.Profile>> inputTable = streamGraph.getTable(inputTableDesc);
      Table<KV<Integer, TestTableData.EnrichedPageView>> outputTable = streamGraph.getTable(outputTableDesc);

      streamGraph.getInputStream("PageView", new NoOpSerde<TestTableData.PageView>())
          .map(pv -> {
              received.add(pv);
              return new KV<Integer, TestTableData.PageView>(pv.getMemberId(), pv);
            })
          .join(inputTable, new TestLocalTable.PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(outputTable);
    };

    runner.run(app);
    runner.waitForFinish();

    Assert.assertEquals(count * partitionCount, received.size());
    Assert.assertEquals(count * partitionCount, writtenRecords.size());
    Assert.assertTrue(writtenRecords.get(0) instanceof TestTableData.EnrichedPageView);
  }

  @Test(expected = SamzaException.class)
  public void testCatchReaderException() {
    TableReadFunction<String, ?> reader = mock(TableReadFunction.class);
    doThrow(new RuntimeException()).when(reader).get(anyString());
    RemoteTable<String, ?> table = new RemoteTable<>("table1", reader);
    table.get("abc");
  }


  @Test(expected = SamzaException.class)
  public void testCatchWriterException() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writer = mock(TableWriteFunction.class);
    doThrow(new RuntimeException()).when(writer).put(anyString(), any());
    RemoteReadWriteTable<String, String> table = new RemoteReadWriteTable<>("table1", reader, writer);
    table.put("abc", "efg");
  }
}
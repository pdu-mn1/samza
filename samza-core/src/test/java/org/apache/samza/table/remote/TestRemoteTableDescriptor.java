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

import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;


public class TestRemoteTableDescriptor {
  @Test
  public void testSerialize() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    desc.withWriteFunction(mock(TableWriteFunction.class));
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTable.READ_FN));
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTable.WRITE_FN));
  }

  @Test
  public void testSerializeNullWriter() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTable.READ_FN));
    Assert.assertFalse(spec.getConfig().containsKey(RemoteTable.WRITE_FN));
  }

  @Test(expected = NullPointerException.class)
  public void testSerializeNullReader() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    TableSpec spec = desc.getTableSpec();
    Assert.assertTrue(spec.getConfig().containsKey(RemoteTable.READ_FN));
  }

  @Test
  public void testDeserializeRecordReader() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(mock(TableReadFunction.class));
    TableSpec spec = desc.getTableSpec();
    RemoteTableProvider provider = new RemoteTableProvider(spec);
    provider.start();
    Table table = provider.getTable();
    Assert.assertTrue(table instanceof RemoteTable);
    Assert.assertNotNull(((RemoteTable) table).readFn);
  }
}

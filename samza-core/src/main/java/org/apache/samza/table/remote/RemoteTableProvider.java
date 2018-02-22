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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provide for remote table instances
 */
public class RemoteTableProvider implements TableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteTableProvider.class);

  private final TableSpec tableSpec;
  private TableReadFunction<?, ?> recordReader;
  private TableWriteFunction<?, ?> recordWriter;
  private SamzaContainerContext containerContext;
  private TaskContext taskContext;

  public RemoteTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    this.containerContext = containerContext;
    this.taskContext = taskContext;
  }

  @Override
  public void start() {
    try {
      recordReader = deserializeObject(tableSpec.getConfig().get(RemoteTable.READ_FN));
      String strWriter = tableSpec.getConfig().getOrDefault(RemoteTable.WRITE_FN, "");
      if (!strWriter.isEmpty()) {
        recordWriter = deserializeObject(tableSpec.getConfig().get(RemoteTable.WRITE_FN));
      }
      LOG.info("Started.");
    } catch (Exception e) {
      String errMsg = "Failed to deserialize " + (recordReader == null ? "TableReadFunction" : "TableWriteFunction");
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopped.");
  }

  private <T> T deserializeObject(String strObject) throws IOException, ClassNotFoundException {
    byte [] bytes = Base64.getDecoder().decode(strObject);
    return (T) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
  }

  @Override
  public Table getTable() {
    ReadableTable table;
    if (this.recordWriter == null) {
      table = new RemoteTable(tableSpec.getId(), recordReader);
    } else {
      table = new RemoteReadWriteTable(tableSpec.getId(), recordReader, recordWriter);
    }
    table.init(containerContext, taskContext);
    return table;
  }

  @Override
  public Map<String, String> generateConfig(Map<String, String> config) {
    Map<String, String> tableConfig = new HashMap<>();

    // Insert table_id prefix to config entires
    tableSpec.getConfig().forEach((k, v) -> {
        String realKey = String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
        tableConfig.put(realKey, v);
      });

    LOG.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }
}


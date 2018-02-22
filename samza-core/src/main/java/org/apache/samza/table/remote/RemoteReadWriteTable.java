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
import org.apache.samza.task.TaskContext;

import com.google.common.base.Preconditions;


/**
 * Remote store backed read writable table
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteReadWriteTable<K, V> extends RemoteTable<K, V> implements ReadWriteTable<K, V> {
  private final TableWriteFunction writeFn;

  public RemoteReadWriteTable(String tableId, TableReadFunction readFn, TableWriteFunction writeFn) {
    super(tableId, readFn);
    Preconditions.checkNotNull(writeFn, "null record writer");
    this.writeFn = writeFn;
  }

  @Override
  public void put(K key, V value) {
    try {
      writeFn.put(key, value);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put a record, key=%s, value=%s", key, value);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void putAll(List<Entry<K, V>> entries) {
    try {
      writeFn.putAll(entries);
    } catch (Exception e) {
      String errMsg = String.format("Failed to put records: %s", entries);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);

    }
  }

  @Override
  public void delete(K key) {
    try {
      writeFn.delete(key);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void deleteAll(List<K> keys) {
    try {
      writeFn.deleteAll(keys);
    } catch (Exception e) {
      String errMsg = String.format("Failed to delete records, keys=%s", keys);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);

    }
  }

  @Override
  public void flush() {
    try {
      writeFn.flush();
    } catch (Exception e) {
      String errMsg = "Failed to flush remote store";
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    super.init(containerContext, taskContext);
    writeFn.init(containerContext, taskContext);
  }

  @Override
  public void close() {
    super.close();
    writeFn.close();
  }
}

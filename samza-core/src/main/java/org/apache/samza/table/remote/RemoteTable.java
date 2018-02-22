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
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Samza {@link org.apache.samza.table.Table} backed by a remote data store or service.
 *
 * Many stream processing applications need to access some kind of remote data stores or RESTful services
 * for each received stream message. Such operation can be naturally modeled as a stream-table join where
 * the store/service is the {@link org.apache.samza.table.Table}. Such abstraction helps application developers
 * focus on the business logic and leave the stream-table join to the framework. Example use cases include:
 * decorating page views with user profile database; scoring page views with impressions services. Having the
 * Table abstraction also enables more code reuse, eg. error handling, rate limiting, caching, when combined
 * other table constructs.
 *
 * RemoteTable provides a base Table representation for all remote stores/services that are intended to be
 * used in stream processing with Samza. It interacts with Samza Table framework and provides pluggable points
 * ({@link TableReadFunction} and {@link TableWriteFunction}) for the actual IO operations with each specific
 * remote store/service. Common {@link TableReadFunction} and {@link TableWriteFunction} implementations for
 * a specific store/service, once created, can be shared by all consuming applications.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 */
public class RemoteTable<K, V> implements ReadableTable<K, V> {
  static final String READ_FN = "io.readFn";
  static final String WRITE_FN = "io.writeFn";

  protected String tableId;
  protected Logger logger;
  protected TableReadFunction<K, V> readFn;

  public RemoteTable(String tableId, TableReadFunction<K, V> readFn) {
    Preconditions.checkArgument(tableId != null && !tableId.isEmpty(), "invalid table id");
    Preconditions.checkNotNull(readFn, "null record reader");
    this.tableId = tableId;
    this.readFn = readFn;
    this.logger = LoggerFactory.getLogger("RemoteTable-" + tableId);
  }

  @Override
  public void init(SamzaContainerContext containerContext, TaskContext context) {
    readFn.init(containerContext, context);
  }

  @Override
  public V get(K key) {
    try {
      return readFn.get(key);
    } catch (Exception e) {
      String errMsg = String.format("Failed to get a record, key=%s", key);
      logger.error(errMsg, e);
      throw new SamzaException(errMsg, e);
    }
  }

  @Override
  public Map<K, V> getAll(List<K> keys) {
    Map<K, V> result;
    try {
      result = readFn.getAll(keys);
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
  public void close() {
    readFn.close();
  }
}

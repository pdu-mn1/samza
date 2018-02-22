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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.task.TaskContext;


/**
 * A function object to be used with a {@link RemoteTable} implementation. It encapsulates the functionality
 * of reading table record(s) for a provided set of key(s).
 *
 * <p> Instances of {@link TableReadFunction} are meant to be serializable. ie. any non-serializable state
 * (eg: network sockets) should be marked as transient and recreated inside readObject().
 *
 * <p> Implementations are expected to be thread-safe.
 * @param <K>
 * @param <V>
 */
@InterfaceStability.Unstable
public interface TableReadFunction<K, V> extends Serializable, ClosableFunction {
  /**
   * Initialize the function with container and task context.
   * @param containerContext container context
   * @param taskContext task context for per-task table; for global table this can be null.
   */
  default void init(SamzaContainerContext containerContext, TaskContext taskContext) {
  }

  /**
   * Fetch single table record for a specified {@code key}. This method must be thread-safe.
   * @param key
   */
  V get(K key);

  /**
   * Fetch the table {@code records} for specified {@code keys}. This method must be thread-safe.
   * @param keys
   * @return all records for the specified keys if succeeded; depending on the implementation
   * of {@link TableReadFunction#get(Object)} it either returns records for a subset of the
   * keys or throws exception when there is any failure.
   */
  default Map<K, V> getAll(Collection<K> keys) {
    Map<K, V> records = new HashMap<>();
    keys.forEach(k -> records.put(k, get(k)));
    return records;
  }

  // optionally implement readObject() to initialize transient states
}
